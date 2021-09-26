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
    REQUIRED = 2
    PREFERRED = 1
    TOLERATED = 0
    REJECTED = -1

    def __int__(self):
        return self.value


class Tag:

    def __init__(self, name, value, tag_type: Enum):
        self.name = name
        self.value = value
        self.tag_type = tag_type

    def __eq__(self, other):
        if not isinstance(other, Tag):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.name == other.name and self.value == other.value and self.tag_type == other.tag_type

    def __repr__(self):
        return f"<Tag: name={self.name}, value={self.value}, type={self.tag_type}>"


@functools.lru_cache(maxsize=8192)
def compile_exec_then_eval(code):
    block = ast.parse(code, mode='exec')
    # assumes last node is an expression
    last = ast.Expression(block.body.pop().value)
    return compile(block, '<string>', mode='exec'), compile(last, '<string>', mode='eval')


# https://stackoverflow.com/a/39381428
def exec_then_eval(code, context):
    exec_block, eval_block = compile_exec_then_eval(str(code))
    locals = dict(globals())
    locals.update(context)
    locals.update({
        'helpers': helpers,
        # Don't unnecessarily compute input_size unless it's referred to
        'input_size': helpers.input_size(context['job']) if 'input_size' in str(code) else 0
    })
    exec(exec_block, locals)
    return eval(eval_block, locals)


class IncompatibleTagsException(Exception):

    def __init__(self, first_set, second_set):
        super().__init__(
            f"Cannot merge tag sets because required and rejected tags mismatch. First tag set requires:"
            f" {first_set.filter(TagType.REQUIRED)} and rejects: {first_set.filter(TagType.REJECTED)}."
            f" Second tag set requires: {second_set.filter(TagType.REQUIRED)} and rejects:"
            f" {second_set.filter(TagType.REJECTED)}.")


class TagSetManager(object):

    def __init__(self, tags=[]):
        self.tags = tags or []

    def add_tag_override(self, tag: Tag):
        # pop the tag if it exists, as a tag can only belong to one type
        self.tags = list(filter(lambda t: t.value != tag.value, self.tags))
        self.tags.append(tag)

    def filter(self, tag_type: TagType) -> list[Tag]:
        return (tag for tag in self.tags if tag.tag_type == tag_type)

    def add_tag_overrides(self, tags: list[Tag]):
        for tag in tags:
            self.add_tag_override(tag)

    def can_merge(self, other: TagSetManager) -> bool:
        self_required = ((t.name, t.value) for t in self.filter(TagType.REQUIRED))
        other_required = ((t.name, t.value) for t in other.filter(TagType.REQUIRED))
        self_rejected = ((t.name, t.value) for t in self.filter(TagType.REJECTED))
        other_rejected = ((t.name, t.value) for t in other.filter(TagType.REJECTED))
        if set(self_required).intersection(set(other_rejected)):
            return False
        elif set(self_rejected).intersection(set(other_required)):
            return False
        else:
            return True

    def extend(self, other) -> TagSetManager:
        assert type(self) == type(other)
        new_tag_set = TagSetManager()
        new_tag_set.add_tag_overrides(other.filter(TagType.TOLERATED))
        new_tag_set.add_tag_overrides(other.filter(TagType.PREFERRED))
        new_tag_set.add_tag_overrides(other.filter(TagType.REQUIRED))
        new_tag_set.add_tag_overrides(other.filter(TagType.REJECTED))
        new_tag_set.add_tag_overrides(self.filter(TagType.TOLERATED))
        new_tag_set.add_tag_overrides(self.filter(TagType.PREFERRED))
        new_tag_set.add_tag_overrides(self.filter(TagType.REQUIRED))
        new_tag_set.add_tag_overrides(self.filter(TagType.REJECTED))
        return new_tag_set

    def merge(self, other: TagSetManager) -> TagSetManager:
        if not self.can_merge(other):
            raise IncompatibleTagsException(self, other)
        new_tag_set = TagSetManager()
        # Add tolerated tags first, as they should be overridden by preferred, required and rejected tags
        new_tag_set.add_tag_overrides(other.filter(TagType.TOLERATED))
        new_tag_set.add_tag_overrides(self.filter(TagType.TOLERATED))
        # Next add preferred, as they should be overridden by required and rejected tags
        new_tag_set.add_tag_overrides(other.filter(TagType.PREFERRED))
        new_tag_set.add_tag_overrides(self.filter(TagType.PREFERRED))
        # Required and rejected tags can be added in either order, as there's no overlap
        new_tag_set.add_tag_overrides(other.filter(TagType.REQUIRED))
        new_tag_set.add_tag_overrides(self.filter(TagType.REQUIRED))
        new_tag_set.add_tag_overrides(other.filter(TagType.REJECTED))
        new_tag_set.add_tag_overrides(self.filter(TagType.REJECTED))
        return new_tag_set

    def match(self, other: TagSetManager) -> bool:
        return (all(other.contains_tag(required) for required in self.filter(TagType.REQUIRED)) and
                all(self.contains_tag(required) for required in other.filter(TagType.REQUIRED)) and
                not any(other.contains_tag(rejected) for rejected in self.filter(TagType.REJECTED)) and
                not any(self.contains_tag(rejected) for rejected in other.filter(TagType.REJECTED)))

    def contains_tag(self, tag) -> bool:
        """
        Returns true if the name and value of the tag match. Ignores tag_type.
        :param tag:
        :return:
        """
        return any(t for t in self.tags if t.name == tag.name and t.value == tag.value)

    def contains_tag_value(self, value) -> bool:
        """
        Returns true if the value of the tag matches. Ignores name and tag_type.
        :param tag:
        :return:
        """
        return any(t for t in self.tags if t.value == value)

    def score(self, other: TagSetManager) -> bool:
        """
        Computes a compatibility score between tag sets.
        :param other:
        :return:
        """
        return (sum(int(tag.tag_type) * int(o.tag_type) for tag in self.tags for o in other.tags
                    if tag.name == o.name and tag.value == o.value)
                # penalize tags that don't exist in the other
                - sum(int(tag.tag_type) for tag in self.tags if not other.contains_tag(tag)))

    def __repr__(self):
        return f"{self.__class__} tags={[tag for tag in self.tags]}"

    @staticmethod
    def from_dict(tags: list[dict]) -> TagSetManager:
        tag_list = []
        for tag_val in tags.get('required') or []:
            tag_list.append(Tag(name="scheduling", value=tag_val, tag_type=TagType.REQUIRED))
        for tag_val in tags.get('preferred') or []:
            tag_list.append(Tag(name="scheduling", value=tag_val, tag_type=TagType.PREFERRED))
        for tag_val in tags.get('tolerated') or []:
            tag_list.append(Tag(name="scheduling", value=tag_val, tag_type=TagType.TOLERATED))
        for tag_val in tags.get('rejected') or []:
            tag_list.append(Tag(name="scheduling", value=tag_val, tag_type=TagType.REJECTED))
        return TagSetManager(tags=tag_list)


class Resource(object):

    def __init__(self, id=None, cores=None, mem=None, env=None, params=None, tags=None, rank=None):
        self.id = id
        self.cores = cores
        self.mem = mem
        self.env = env
        self.params = params
        self.tags = TagSetManager.from_dict(tags or {})
        self.rank = rank

    def __repr__(self):
        return f"{self.__class__} id={self.id}, cores={self.cores}, mem={self.mem}, " \
               f"env={self.env}, params={self.params}, tags={self.tags}, rank={self.rank[:10] if self.rank else ''}"

    def override(self, resource):
        new_resource = copy.copy(resource)
        new_resource.id = self.id or resource.id
        new_resource.cores = self.cores or resource.cores
        new_resource.mem = self.mem or resource.mem
        new_resource.env = resource.env or {}
        new_resource.env.update(self.env or {})
        new_resource.params = resource.params or {}
        new_resource.params.update(self.params or {})
        new_resource.rank = self.rank or resource.rank
        return new_resource

    def extend(self, resource):
        new_resource = self.override(resource)
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
        new_resource = resource.override(self)
        new_resource.tags = self.tags.merge(resource.tags)
        return new_resource

    def match(self, destination, context):
        """
        The match operation checks whether all of the required tags in a resource are present
        in the destination resource, and none of the rejected tags in the first resource are
        present in the second resource.

        This is used to check compatibility of a final set of merged tool requirements with its destination.

        :param destination:
        :return:
        """
        if destination.cores and destination.cores < self.cores:
            return False
        if destination.mem and destination.mem < self.mem:
            return False
        return self.tags.match(destination.tags or {})

    def evaluate(self, context):
        new_resource = copy.copy(self)
        if self.cores:
            new_resource.cores = exec_then_eval(self.cores, context)
            context['cores'] = new_resource.cores
        if self.mem:
            new_resource.mem = exec_then_eval(self.mem, context)
            context['mem'] = new_resource.mem
        if self.env:
            evaluated_env = {}
            for key, entry in self.env.items():
                # evaluate as an f-string
                entry = "f'''" + str(entry) + "'''"
                evaluated_env[key] = exec_then_eval(entry, context)
            new_resource.env = evaluated_env
            context['env'] = new_resource.env
        if self.params:
            evaluated_params = {}
            for key, param in self.params.items():
                # evaluate as an f-string
                param = "f'''" + str(param) + "'''"
                evaluated_params[key] = exec_then_eval(param, context)
            new_resource.params = evaluated_params
            context['params'] = new_resource.params
        return new_resource

    def rank_destinations(self, destinations, context):
        if self.rank:
            context['candidate_destinations'] = destinations
            return exec_then_eval(self.rank, context)
        else:
            # Just return in whatever order the destinations
            # were originally found
            return destinations

    def score(self, resource):
        score = self.tags.score(resource.tags)
        return score


class ResourceWithRules(Resource):

    def __init__(self, id=None, cores=None, mem=None, env=None, params=None, tags=None, rank=None, rules=None):
        super().__init__(id, cores, mem, env, params, tags, rank)
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
            params=resource_dict.get('params'),
            tags=resource_dict.get('scheduling'),
            rank=resource_dict.get('rank'),
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
        new_resource = super().evaluate(context)
        for rule in new_resource.rules:
            evaluated = rule.evaluate(context)
            if evaluated:
                new_resource = evaluated.extend(new_resource)
        return new_resource

    def __repr__(self):
        return super().__repr__() + f", rules={self.rules}"


class Tool(ResourceWithRules):

    def __init__(self, id=None, cores=None, mem=None, env=None, params=None, tags=None, rank=None, rules=None):
        super().__init__(id, cores, mem, env, params, tags, rank, rules)


class User(ResourceWithRules):

    def __init__(self, id=None, cores=None, mem=None, env=None, params=None, tags=None, rank=None, rules=None):
        super().__init__(id, cores, mem, env, params, tags, rank, rules)


class Role(ResourceWithRules):

    def __init__(self, id=None, cores=None, mem=None, env=None, params=None, tags=None, rank=None, rules=None):
        super().__init__(id, cores, mem, env, params, tags, rank, rules)


class Destination(ResourceWithRules):

    def __init__(self, id=None, cores=None, mem=None, env=None, params=None, tags=None, rules=None):
        super().__init__(id, cores, mem, env, params, tags, rules=rules)

    @staticmethod
    def from_dict(resource_dict):
        return Destination(
            id=resource_dict.get('id'),
            cores=resource_dict.get('cores'),
            mem=resource_dict.get('mem'),
            env=resource_dict.get('env'),
            params=resource_dict.get('params'),
            tags=resource_dict.get('scheduling'),
            rules=resource_dict.get('rules')
        )


class Rule(Resource):

    def __init__(self, id=None, cores=None, mem=None, env=None, params=None, tags=None, match=None, fail=None):
        super().__init__(id, cores, mem, env, params, tags)
        self.match = match
        self.fail = fail

    @staticmethod
    def from_dict(resource_dict):
        return Rule(
            id=resource_dict.get('id'),
            cores=resource_dict.get('cores'),
            mem=resource_dict.get('mem'),
            env=resource_dict.get('env'),
            params=resource_dict.get('params'),
            tags=resource_dict.get('scheduling'),
            match=resource_dict.get('match'),
            fail=resource_dict.get('fail')
        )

    def __repr__(self):
        return super().__repr__() + f", match={self.match}, fail={self.fail}"

    def evaluate(self, context):
        try:
            if exec_then_eval(self.match, context):
                if self.fail:
                    raise JobMappingException(
                        exec_then_eval("f'''" + str(self.fail) + "'''", context))
                return super().evaluate(context)
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
        self.destinations = validated.get('destinations')

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
            'roles': self.validate_resources(Role, destination_data.get('roles', {})),
            'destinations': self.validate_resources(Destination, destination_data.get('destinations', {}))
        }
        return validated

    @staticmethod
    def from_file_path(path: str):
        with open(path, 'r') as f:
            dest_data = yaml.safe_load(f)
            return ResourceDestinationParser(dest_data)
