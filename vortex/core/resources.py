from __future__ import annotations

from enum import Enum
import logging
import copy
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


class IncompatibleTagsException(Exception):

    def __init__(self, first_set, second_set):

        super().__init__(
            f"Cannot merge tag sets because required and rejected tags mismatch. First tag set requires:"
            f" {[tag.value for tag in first_set.filter(TagType.REQUIRED)]} and rejects:"
            f" {[tag.value for tag in first_set.filter(TagType.REJECTED)]}. Second tag set requires:"
            f" {[tag.value for tag in second_set.filter(TagType.REQUIRED)]} and rejects:"
            f" {[tag.value for tag in second_set.filter(TagType.REJECTED)]}.")


class TagSetManager(object):

    def __init__(self, tags=[]):
        self.tags = tags or []

    def add_tag_override(self, tag: Tag):
        # pop the tag if it exists, as a tag can only belong to one type
        self.tags = list(filter(lambda t: t.value != tag.value, self.tags))
        self.tags.append(tag)

    def filter(self, tag_type: TagType | list[TagType] = None,
               tag_name: str = None, tag_value: str = None) -> list[Tag]:
        filtered = self.tags
        if tag_type:
            if isinstance(tag_type, TagType):
                filtered = (tag for tag in filtered if tag.tag_type == tag_type)
            else:
                filtered = (tag for tag in filtered if tag.tag_type in tag_type)
        if tag_name:
            filtered = (tag for tag in filtered if tag.name == tag_name)
        if tag_value:
            filtered = (tag for tag in filtered if tag.value == tag_value)
        return filtered

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
        return any(self.filter(tag_name=tag.name, tag_value=tag.value))

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

    def __init__(self, loader, id=None, cores=None, mem=None, gpus=None, env=None, params=None, tags=None, rank=None,
                 inherits=None):
        self.loader = loader
        self.id = id
        self.cores = cores
        self.mem = mem
        self.gpus = gpus
        self.env = env
        self.params = params
        self.tags = TagSetManager.from_dict(tags or {})
        self.rank = rank
        self.inherits = inherits
        self.validate()

    def validate(self):
        """
        Validates each code block and makes sure the code can be compiled.
        This process also results in the compiled code being cached by the loader,
        so that future evaluations are faster.
        """
        if self.cores:
            self.loader.compile_code_block(self.cores)
        if self.mem:
            self.loader.compile_code_block(self.mem)
        if self.gpus:
            self.loader.compile_code_block(self.gpus)
        if self.env:
            for key, entry in self.env.items():
                self.loader.compile_code_block(entry, as_f_string=True)
        if self.params:
            for key, param in self.params.items():
                self.loader.compile_code_block(param, as_f_string=True)
        if self.rank:
            self.loader.compile_code_block(self.rank)

    def __repr__(self):
        return f"{self.__class__} id={self.id}, cores={self.cores}, mem={self.mem}, gpus={self.gpus}, " \
               f"env={self.env}, params={self.params}, tags={self.tags}, rank={self.rank[:10] if self.rank else ''}, " \
               f"inherits={self.inherits}"

    def override(self, resource):
        new_resource = copy.copy(resource)
        new_resource.id = self.id or resource.id
        new_resource.cores = self.cores or resource.cores
        new_resource.mem = self.mem or resource.mem
        new_resource.gpus = self.gpus or resource.gpus
        new_resource.env = resource.env or {}
        new_resource.env.update(self.env or {})
        new_resource.params = resource.params or {}
        new_resource.params.update(self.params or {})
        new_resource.rank = self.rank if self.rank is not None else resource.rank
        new_resource.inherits = self.inherits if self.inherits is not None else resource.inherits
        return new_resource

    def extend(self, resource):
        if resource:
            new_resource = self.override(resource)
            new_resource.tags = self.tags.extend(resource.tags)
            return new_resource
        else:
            return self

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

    def matches(self, destination, context):
        """
        The match operation checks whether all of the required tags in a resource are present
        in the destination resource, and none of the rejected tags in the first resource are
        present in the second resource.

        This is used to check compatibility of a final set of merged tool requirements with its destination.

        :param destination:
        :return:
        """
        if destination.cores and self.cores and destination.cores < self.cores:
            return False
        if destination.mem and self.mem and destination.mem < self.mem:
            return False
        if destination.gpus and self.gpus and destination.gpus < self.gpus:
            return False
        return self.tags.match(destination.tags or {})

    def evaluate(self, context):
        new_resource = copy.copy(self)
        if self.cores:
            new_resource.cores = self.loader.eval_code_block(self.cores, context)
            context['cores'] = new_resource.cores
        if self.mem:
            new_resource.mem = self.loader.eval_code_block(self.mem, context)
            context['mem'] = new_resource.mem
        if self.gpus:
            new_resource.gpus = self.loader.eval_code_block(self.gpus, context)
            context['gpus'] = new_resource.gpus
        if self.env:
            evaluated_env = {}
            for key, entry in self.env.items():
                evaluated_env[key] = self.loader.eval_code_block(entry, context, as_f_string=True)
            new_resource.env = evaluated_env
            context['env'] = new_resource.env
        if self.params:
            evaluated_params = {}
            for key, param in self.params.items():
                evaluated_params[key] = self.loader.eval_code_block(param, context, as_f_string=True)
            new_resource.params = evaluated_params
            context['params'] = new_resource.params
        return new_resource

    def rank_destinations(self, destinations, context):
        if self.rank:
            log.debug(f"Ranking destinations: {destinations} for resource: {self} using custom function")
            context['candidate_destinations'] = destinations
            return self.loader.eval_code_block(self.rank, context)
        else:
            # Sort destinations by priority
            log.debug(f"Ranking destinations: {destinations} for resource: {self} using custom default ranker")
            return sorted(destinations, key=lambda d: d.score(self), reverse=True)

    def score(self, resource):
        score = self.tags.score(resource.tags)
        log.debug(f"Destination: {resource} scored: {score}")
        return score


class ResourceWithRules(Resource):

    def __init__(self, loader, id=None, cores=None, mem=None, gpus=None, env=None,
                 params=None, tags=None, rank=None, inherits=None, rules=None):
        super().__init__(loader, id, cores, mem, gpus, env, params, tags, rank, inherits)
        self.rules = self.validate_rules(rules)

    def validate_rules(self, rules: list) -> list:
        validated = {}
        for rule in rules or []:
            try:
                validated_rule = Rule.from_dict(self.loader, rule)
                validated[validated_rule.id] = validated_rule
            except Exception:
                log.exception(f"Could not load rule for resource: {self.__class__} with id: {self.id} and data: {rule}")
        return validated

    @classmethod
    def from_dict(cls: type, loader, resource_dict):
        return cls(
            loader=loader,
            id=resource_dict.get('id'),
            cores=resource_dict.get('cores'),
            mem=resource_dict.get('mem'),
            gpus=resource_dict.get('gpus'),
            env=resource_dict.get('env'),
            params=resource_dict.get('params'),
            tags=resource_dict.get('scheduling'),
            rank=resource_dict.get('rank'),
            inherits=resource_dict.get('inherits'),
            rules=resource_dict.get('rules')
        )

    def override(self, resource):
        new_resource = super().override(resource)
        new_resource.rules = copy.copy(resource.rules)
        new_resource.rules.update(self.rules or {})
        for rule in self.rules.values():
            if resource.rules.get(rule.id):
                new_resource.rules[rule.id] = rule.extend(resource.rules[rule.id])
        return new_resource

    def evaluate(self, context):
        new_resource = self
        for rule in self.rules.values():
            if rule.is_matching(context):
                new_resource = rule.extend(new_resource)
        return super(ResourceWithRules, new_resource).evaluate(context)

    def __repr__(self):
        return super().__repr__() + f", rules={self.rules}"


class Tool(ResourceWithRules):

    def __init__(self, loader, id=None, cores=None, mem=None, gpus=None,
                 env=None, params=None, tags=None, rank=None, inherits=None, rules=None):
        super().__init__(loader, id, cores, mem, gpus, env, params, tags, rank, inherits, rules)


class User(ResourceWithRules):

    def __init__(self, loader, id=None, cores=None, mem=None, gpus=None,
                 env=None, params=None, tags=None, rank=None, inherits=None, rules=None):
        super().__init__(loader, id, cores, mem, gpus, env, params, tags, rank, inherits, rules)


class Role(ResourceWithRules):

    def __init__(self, loader, id=None, cores=None, mem=None, gpus=None,
                 env=None, params=None, tags=None, rank=None, inherits=None, rules=None):
        super().__init__(loader, id, cores, mem, gpus, env, params, tags, rank, inherits, rules)


class Destination(ResourceWithRules):

    def __init__(self, loader, id=None, cores=None, mem=None, gpus=None,
                 env=None, params=None, tags=None, inherits=None, rules=None):
        super().__init__(loader, id, cores, mem, gpus, env, params, tags, inherits, rules=rules)

    @staticmethod
    def from_dict(loader, resource_dict):
        return Destination(
            loader=loader,
            id=resource_dict.get('id'),
            cores=resource_dict.get('cores'),
            mem=resource_dict.get('mem'),
            gpus=resource_dict.get('gpus'),
            env=resource_dict.get('env'),
            params=resource_dict.get('params'),
            tags=resource_dict.get('scheduling'),
            inherits=resource_dict.get('inherits'),
            rules=resource_dict.get('rules')
        )


class Rule(Resource):

    rule_counter = 0

    def __init__(self, loader, id=None, cores=None, mem=None, gpus=None,
                 env=None, params=None, tags=None, inherits=None, match=None, fail=None):
        if not id:
            Rule.rule_counter += 1
            id = f"vortex_rule_{Rule.rule_counter}"
        super().__init__(loader, id, cores, mem, gpus, env, params, tags, inherits=inherits)
        self.match = match
        self.fail = fail
        if self.match:
            self.loader.compile_code_block(self.match)
        if self.fail:
            self.loader.compile_code_block(self.fail, as_f_string=True)

    @staticmethod
    def from_dict(loader, resource_dict):
        return Rule(
            loader=loader,
            id=resource_dict.get('id'),
            cores=resource_dict.get('cores'),
            mem=resource_dict.get('mem'),
            gpus=resource_dict.get('gpus'),
            env=resource_dict.get('env'),
            params=resource_dict.get('params'),
            tags=resource_dict.get('scheduling'),
            inherits=resource_dict.get('inherits'),
            match=resource_dict.get('match'),
            fail=resource_dict.get('fail')
        )

    def override(self, resource):
        new_resource = super().override(resource)
        new_resource.match = self.match if self.match is not None else getattr(resource, 'match', None)
        new_resource.fail = self.fail if self.fail is not None else getattr(resource, 'fail', None)
        return new_resource

    def __repr__(self):
        return super().__repr__() + f", match={self.match}, fail={self.fail}"

    def is_matching(self, context):
        try:
            if self.loader.eval_code_block(self.match, context):
                if self.fail:
                    raise JobMappingException(
                        self.loader.eval_code_block(self.fail, context, as_f_string=True))
                return True
            else:
                return False
        except JobMappingException:
            raise
        except Exception as e:
            raise Exception(f"Error evaluating rule: {self}") from e
