from __future__ import annotations

import ast
from enum import Enum
import functools
import logging
import yaml
import copy
from . import helpers
from galaxy.jobs.mapper import JobMappingException


log = logging.getLogger(__name__)


class TagType(Enum):
    REQUIRED = "required"
    PREFERRED = "preferred"
    TOLERATED = "tolerated"
    REJECTED = "rejected"


@functools.lru_cache(maxsize=8192)
def compile_exec_then_eval(code):
    block = ast.parse(code, mode='exec')
    # assumes last node is an expression
    last = ast.Expression(block.body.pop().value)
    return compile(block, '<string>', mode='exec'), compile(last, '<string>', mode='eval')


# https://stackoverflow.com/a/39381428
def exec_then_eval(code, context):
    exec_block, eval_block = compile_exec_then_eval(str(code))
    _globals = globals()
    _locals = context

    _locals.update({
        'helpers': helpers,
        # Don't unnecessarily compute input_size unless it's referred to
        'input_size': helpers.input_size(context['job']) if 'input_size' in str(code) else 0
    })
    exec(exec_block, _globals, _locals)
    return eval(eval_block, _globals, _locals)


class IncompatibleTagsException(Exception):

    def __init__(self, first_set, second_set):
        super().__init__(
            f"Cannot merge tag sets because required and rejected tags mismatch. First tag set requires:"
            f" {first_set.required_tags} and rejects: {first_set.rejected_tags}. Second tag set requires:"
            f" {second_set.required_tags} and rejects: {second_set.rejected_tags}.")


class TagSetManager(object):

    def __init__(self, required=[], preferred=[], tolerated=[], rejected=[]):
        self.required_tags = required or []
        self.preferred_tags = preferred or []
        self.tolerated_tags = tolerated or []
        self.rejected_tags = rejected or []

    def add_tag_override(self, tag: str, tag_type: TagType):
        # pop the tag if it exists, as a tag can only belong to one category
        self.required_tags = list(filter(lambda x: x != tag, self.required_tags))
        self.preferred_tags = list(filter(lambda x: x != tag, self.preferred_tags))
        self.tolerated_tags = list(filter(lambda x: x != tag, self.tolerated_tags))
        self.rejected_tags = list(filter(lambda x: x != tag, self.rejected_tags))

        if tag_type == TagType.REQUIRED:
            self.required_tags.append(tag)
        elif tag_type == TagType.PREFERRED:
            self.preferred_tags.append(tag)
        elif tag_type == TagType.TOLERATED:
            self.tolerated_tags.append(tag)
        elif tag_type == TagType.REJECTED:
            self.rejected_tags.append(tag)
        else:
            raise Exception(f"Unrecognized tag type: {tag_type} for tag: {tag}")

    def add_tag_overrides(self, tags: list[str], tag_type: TagType):
        for tag in tags:
            self.add_tag_override(tag, tag_type)

    def can_merge(self, other: TagSetManager):
        if set(self.required_tags).intersection(set(other.rejected_tags)):
            return False
        elif set(self.rejected_tags).intersection(set(other.required_tags)):
            return False
        else:
            return True

    def extend(self, other):
        assert type(self) == type(other)
        new_tag_set = TagSetManager()
        new_tag_set.add_tag_overrides(other.tolerated_tags, TagType.TOLERATED)
        new_tag_set.add_tag_overrides(other.preferred_tags, TagType.PREFERRED)
        new_tag_set.add_tag_overrides(other.required_tags, TagType.REQUIRED)
        new_tag_set.add_tag_overrides(other.rejected_tags, TagType.REJECTED)
        new_tag_set.add_tag_overrides(self.tolerated_tags, TagType.TOLERATED)
        new_tag_set.add_tag_overrides(self.preferred_tags, TagType.PREFERRED)
        new_tag_set.add_tag_overrides(self.required_tags, TagType.REQUIRED)
        new_tag_set.add_tag_overrides(self.rejected_tags, TagType.REJECTED)
        return new_tag_set

    def merge(self, other: TagSetManager):
        if not self.can_merge(other):
            raise IncompatibleTagsException(self, other)
        new_tag_set = TagSetManager()
        # Add tolerated tags first, as they should be overridden by preferred, required and rejected tags
        new_tag_set.add_tag_overrides(other.tolerated_tags, TagType.TOLERATED)
        new_tag_set.add_tag_overrides(self.tolerated_tags, TagType.TOLERATED)
        # Next add preferred, as they should be overridden by required and rejected tags
        new_tag_set.add_tag_overrides(other.preferred_tags, TagType.PREFERRED)
        new_tag_set.add_tag_overrides(self.preferred_tags, TagType.PREFERRED)
        # Required and rejected tags can be added in either order, as there's no overlap
        new_tag_set.add_tag_overrides(other.required_tags, TagType.REQUIRED)
        new_tag_set.add_tag_overrides(self.required_tags, TagType.REQUIRED)
        new_tag_set.add_tag_overrides(other.rejected_tags, TagType.REJECTED)
        new_tag_set.add_tag_overrides(self.rejected_tags, TagType.REJECTED)
        return new_tag_set

    def match(self, tags):
        return (all(required in tags for required in self.required_tags) and
                not any(rejected in tags for rejected in self.rejected_tags))

    @staticmethod
    def from_dict(tags):
        return TagSetManager(tags.get('required'), tags.get('preferred'), tags.get('tolerated'), tags.get('rejected'))


class Resource(object):

    def __init__(self, id=None, cores=None, mem=None, env=None, tags=None):
        self.id = id
        self.cores = cores
        self.mem = mem
        self.env = env
        self.tags = TagSetManager.from_dict(tags or {})

    def __repr__(self):
        return f"{self.__class__} id={self.id}, cores={self.cores}, mem={self.mem}, env={self.env}, tags={self.tags}"

    def extend(self, resource):
        new_resource = copy.copy(resource)
        new_resource.id = self.id or resource.id
        new_resource.cores = self.cores or resource.cores
        new_resource.mem = self.mem or resource.mem
        new_resource.env = self.env or resource.env
        new_resource.tags = self.tags.extend(resource.tags)
        return new_resource

    def merge(self, resource):
        """
        The merge operation takes a resource and merges its requirements with a second resource, with
        the second resource overriding the requirements of the first resource.
        For example, a User resource and a Tool resource can be merged to create a combined resource that contain
        both their mutual requirements, as long as they do not define mutually incompatible requirements.
        For example, the User requires the "pulsar" tag, but the tool rejects the "pulsar" tag.
        In this case, an IncompatibleTagsException will be thrown.

        The general hierarchy of resources in vortex is Tool > User > Role and therefore, these resources
        are usually merged as: role.merge(user).merge(tool), to produce a final set of tool requirements.

        The merged requirements can then be matched against the destination, through the match operation.

        :param resource:
        :return:
        """
        new_resource = copy.copy(self)
        new_resource.id = resource.id or self.id
        new_resource.cores = resource.cores or self.cores
        new_resource.mem = resource.mem or self.mem
        new_resource.env = resource.env or self.env
        new_resource.tags = self.tags.merge(resource.tags)
        return new_resource

    def matches_destination(self, destination, context):
        """
        The match operation checks whether all of the required tags in a resource are present
        in the destination resource, and none of the rejected tags in the first resource are
        present in the second resource.

        This is used to check compatibility of a final set of merged tool requirements with its destination.

        :param destination:
        :return:
        """
        return self.tags.match(destination.params.get('vortex_tags') or {})


class ResourceWithRules(Resource):

    def __init__(self, id=None, cores=None, mem=None, env=None, tags=None, rules=None):
        super().__init__(id, cores, mem, env, tags)
        self.rules = self.validate(rules)

    def validate(self, rules: list) -> list:
        validated = []
        for rule in rules or []:
            try:
                validated.append(Rule.from_dict(rule))
            except Exception:
                log.exception(f"Could not load rule for resource: {self.__class__} with id: {self.id} and data: {rule}")
        return validated

    @classmethod
    def from_dict(cls: type, resource_dict):
        return cls(
            id=resource_dict.get('id'),
            cores=resource_dict.get('cores'),
            mem=resource_dict.get('mem'),
            env=resource_dict.get('env'),
            tags=resource_dict.get('tags'),
            rules=resource_dict.get('rules')
        )

    def extend(self, resource):
        new_resource = super().extend(resource)
        new_resource.rules = (self.rules or []) + (resource.rules or [])
        return new_resource

    def merge(self, resource):
        new_resource = super().merge(resource)
        new_resource.rules = (self.rules or []) + (resource.rules or [])
        return new_resource

    def evaluate(self, context):
        new_resource = copy.copy(self)
        if self.cores:
            new_resource.cores = exec_then_eval(self.cores, context)
            context['cores'] = new_resource.cores
        if self.mem:
            new_resource.mem = exec_then_eval(self.mem, context)
            context['mem'] = new_resource.mem
        for rule in new_resource.rules:
            evaluated = rule.evaluate(context)
            if evaluated:
                new_resource.cores = evaluated.get('cores') or self.cores
                new_resource.mem = evaluated.get('mem') or self.cores
                new_resource.tags = evaluated['tags'].extend(self.tags)
                new_resource.env = new_resource.env or {}
                new_resource.env.update(evaluated.get('env') or {})
        return new_resource

    def __repr__(self):
        return super().__repr__() + f", rules={self.rules}"


class Tool(ResourceWithRules):

    def __init__(self, id=None, cores=None, mem=None, env=None, tags=None, rules=None):
        super().__init__(id, cores, mem, env, tags, rules)


class User(ResourceWithRules):

    def __init__(self, id=None, cores=None, mem=None, env=None, tags=None, rules=None):
        super().__init__(id, cores, mem, env, tags, rules)


class Role(ResourceWithRules):

    def __init__(self, id=None, cores=None, mem=None, env=None, tags=None, rules=None):
        super().__init__(id, cores, mem, env, tags, rules)


class Destination(ResourceWithRules):

    def __init__(self, id=None, cores=None, mem=None, env=None, tags=None, rules=None):
        super().__init__(id, cores, mem, env, tags, rules)


class Rule(Resource):

    def __init__(self, id=None, cores=None, mem=None, env=None, tags=None, match=None, fail=None):
        super().__init__(id, cores, mem, env, tags)
        self.match = match
        self.fail = fail

    @staticmethod
    def from_dict(resource_dict):
        return Rule(
            id=resource_dict.get('id'),
            cores=resource_dict.get('cores'),
            mem=resource_dict.get('mem'),
            env=resource_dict.get('env'),
            tags=resource_dict.get('tags'),
            match=resource_dict.get('match'),
            fail=resource_dict.get('fail')
        )

    def __repr__(self):
        return super().__repr__() + f", match={self.match}, fail={self.fail}"

    def evaluate(self, context):
        try:
            if exec_then_eval(self.match, context):
                if self.fail:
                    raise JobMappingException(self.fail)
                cores = None
                mem = None
                if self.cores:
                    cores = exec_then_eval(self.cores, context)
                    context['cores'] = cores
                if self.mem:
                    mem = exec_then_eval(self.mem, context)
                    context['mem'] = mem
                return {
                    'cores': cores,
                    'mem': mem,
                    'env': self.env or {},
                    'tags': self.tags,
                }
            else:
                return {}
        except JobMappingException:
            raise
        except Exception as e:
            raise Exception(f"Error evaluating rule: {self.match}") from e


class ResourceDestinationParser(object):

    def __init__(self, destination_data: dict):
        validated = self.validate(destination_data)
        self.tools = validated.get('tools')
        self.users = validated.get('users')
        self.roles = validated.get('roles')

    @staticmethod
    def validate_resources(resource_class: type, resource_list: dict) -> dict:
        validated = {}
        for resource_id, resource_dict in resource_list.items():
            try:
                resource_dict['id'] = resource_id
                validated[resource_id] = resource_class.from_dict(resource_dict)
            except Exception:
                log.exception(f"Could not load resource of type: {resource_class} with data: {resource_dict}")
        return validated

    def validate(self, destination_data: dict) -> dict:
        validated = {
         'tools': self.validate_resources(Tool, destination_data.get('tools', {})),
         'users': self.validate_resources(User, destination_data.get('users', {})),
         'roles': self.validate_resources(Role, destination_data.get('roles', {}))
        }
        return validated

    @staticmethod
    def from_file_path(path: str):
        with open(path, 'r') as f:
            dest_data = yaml.safe_load(f)
            return ResourceDestinationParser(dest_data)
