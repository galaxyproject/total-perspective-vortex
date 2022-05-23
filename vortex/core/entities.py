from __future__ import annotations

from enum import Enum
import logging
import copy


log = logging.getLogger(__name__)


class TagType(Enum):
    REQUIRE = 2
    PREFER = 1
    ACCEPT = 0
    REJECT = -1

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
            f"Cannot combine tag sets because require and reject tags mismatch. First tag set requires:"
            f" {[tag.value for tag in first_set.filter(TagType.REQUIRE)]} and rejects:"
            f" {[tag.value for tag in first_set.filter(TagType.REJECT)]}. Second tag set requires:"
            f" {[tag.value for tag in second_set.filter(TagType.REQUIRE)]} and rejects:"
            f" {[tag.value for tag in second_set.filter(TagType.REJECT)]}.")


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

    def can_combine(self, other: TagSetManager) -> bool:
        self_required = ((t.name, t.value) for t in self.filter(TagType.REQUIRE))
        other_required = ((t.name, t.value) for t in other.filter(TagType.REQUIRE))
        self_rejected = ((t.name, t.value) for t in self.filter(TagType.REJECT))
        other_rejected = ((t.name, t.value) for t in other.filter(TagType.REJECT))
        if set(self_required).intersection(set(other_rejected)):
            return False
        elif set(self_rejected).intersection(set(other_required)):
            return False
        else:
            return True

    def inherit(self, other) -> TagSetManager:
        assert type(self) == type(other)
        new_tag_set = TagSetManager()
        new_tag_set.add_tag_overrides(other.filter(TagType.ACCEPT))
        new_tag_set.add_tag_overrides(other.filter(TagType.PREFER))
        new_tag_set.add_tag_overrides(other.filter(TagType.REQUIRE))
        new_tag_set.add_tag_overrides(other.filter(TagType.REJECT))
        new_tag_set.add_tag_overrides(self.filter(TagType.ACCEPT))
        new_tag_set.add_tag_overrides(self.filter(TagType.PREFER))
        new_tag_set.add_tag_overrides(self.filter(TagType.REQUIRE))
        new_tag_set.add_tag_overrides(self.filter(TagType.REJECT))
        return new_tag_set

    def combine(self, other: TagSetManager) -> TagSetManager:
        if not self.can_combine(other):
            raise IncompatibleTagsException(self, other)
        new_tag_set = TagSetManager()
        # Add accept tags first, as they should be overridden by prefer, require and reject tags
        new_tag_set.add_tag_overrides(other.filter(TagType.ACCEPT))
        new_tag_set.add_tag_overrides(self.filter(TagType.ACCEPT))
        # Next add preferred, as they should be overridden by require and reject tags
        new_tag_set.add_tag_overrides(other.filter(TagType.PREFER))
        new_tag_set.add_tag_overrides(self.filter(TagType.PREFER))
        # Require and reject tags can be added in either order, as there's no overlap
        new_tag_set.add_tag_overrides(other.filter(TagType.REQUIRE))
        new_tag_set.add_tag_overrides(self.filter(TagType.REQUIRE))
        new_tag_set.add_tag_overrides(other.filter(TagType.REJECT))
        new_tag_set.add_tag_overrides(self.filter(TagType.REJECT))
        return new_tag_set

    def match(self, other: TagSetManager) -> bool:
        return (all(other.contains_tag(required) for required in self.filter(TagType.REQUIRE)) and
                all(self.contains_tag(required) for required in other.filter(TagType.REQUIRE)) and
                not any(other.contains_tag(rejected) for rejected in self.filter(TagType.REJECT)) and
                not any(self.contains_tag(rejected) for rejected in other.filter(TagType.REJECT)))

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
        for tag_val in tags.get('require') or []:
            tag_list.append(Tag(name="scheduling", value=tag_val, tag_type=TagType.REQUIRE))
        for tag_val in tags.get('prefer') or []:
            tag_list.append(Tag(name="scheduling", value=tag_val, tag_type=TagType.PREFER))
        for tag_val in tags.get('accept') or []:
            tag_list.append(Tag(name="scheduling", value=tag_val, tag_type=TagType.ACCEPT))
        for tag_val in tags.get('reject') or []:
            tag_list.append(Tag(name="scheduling", value=tag_val, tag_type=TagType.REJECT))
        return TagSetManager(tags=tag_list)


class Entity(object):

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

    def override(self, entity):
        new_entity = copy.copy(entity)
        new_entity.id = self.id or entity.id
        new_entity.cores = self.cores or entity.cores
        new_entity.mem = self.mem or entity.mem
        new_entity.gpus = self.gpus or entity.gpus
        new_entity.env = copy.copy(entity.env) or {}
        new_entity.env.update(self.env or {})
        new_entity.params = copy.copy(entity.params) or {}
        new_entity.params.update(self.params or {})
        new_entity.rank = self.rank if self.rank is not None else entity.rank
        new_entity.inherits = self.inherits if self.inherits is not None else entity.inherits
        return new_entity

    def inherit(self, entity):
        if entity:
            new_entity = self.override(entity)
            new_entity.tags = self.tags.inherit(entity.tags)
            return new_entity
        else:
            return copy.deepcopy(self)

    def combine(self, entity):
        """
        The combine operation takes an entity and combines its requirements with a second entity.
        For example, a User entity and a Tool entity can be combined to create a merged entity that contain
        both their mutual requirements, as long as they do not define mutually incompatible requirements.
        For example, if a User requires the "pulsar" tag, but the tool rejects the "pulsar" tag.
        In this case, an IncompatibleTagsException will be thrown.

        If both entities define cpu, memory and gpu requirements, the lower of those requirements are used.
        This provides a mechanism for limiting the maximum memory used by a particular Group or User.

        The general hierarchy of entities in vortex is User > Role > Tool and therefore, these entity
        are usually merged as: tool.combine(role).combine(user), to produce a final set of tool requirements.

        The combined requirements can then be matched against the destination, through the match operation.

        :param entity:
        :return:
        """
        new_entity = entity.override(self)
        if self.cores and entity.cores:
            new_entity.cores = min(self.cores, entity.cores)
        if self.mem and entity.mem:
            new_entity.mem = min(self.mem, entity.mem)
        if self.gpus and entity.gpus:
            new_entity.gpus = min(self.gpus, entity.gpus)
        new_entity.id = f"{type(self).__name__}: {self.id}, {type(entity).__name__}: {entity.id}"
        new_entity.tags = self.tags.combine(entity.tags)
        return new_entity

    def matches(self, destination, context):
        """
        The match operation checks whether all of the require tags in an entity are present
        in the destination entity, and none of the reject tags in the first entity are
        present in the second entity.

        This is used to check compatibility of a final set of combined tool requirements with its destination.

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

    def evaluate_early(self, context):
        """
        Evaluate expressions in entity properties that must be evaluated early, which
        is to say, evaluated prior to combining entity requirements. These properties
        are namely, cores, mem and gpus, since at the time of combining entity requirements,
        the properties must be compared.
        :param context:
        :return:
        """
        new_entity = copy.deepcopy(self)
        if self.gpus:
            new_entity.gpus = self.loader.eval_code_block(self.gpus, context)
            context['gpus'] = new_entity.gpus
        if self.cores:
            new_entity.cores = self.loader.eval_code_block(self.cores, context)
            context['cores'] = new_entity.cores
        if self.mem:
            new_entity.mem = self.loader.eval_code_block(self.mem, context)
        return new_entity

    def evaluate_late(self, context):
        """
        Evaluate expressions in entity properties that must be evaluated as late as possible, which is
        to say, after combining entity requirements. This includes env and params, that rely on
        properties such as cores, mem and gpus after they are combined.
        :param context:
        :return:
        """
        new_entity = copy.deepcopy(self)
        context['gpus'] = new_entity.gpus
        context['cores'] = new_entity.cores
        context['mem'] = new_entity.mem
        if self.env:
            evaluated_env = {}
            for key, entry in self.env.items():
                evaluated_env[key] = self.loader.eval_code_block(entry, context, as_f_string=True)
            new_entity.env = evaluated_env
            context['env'] = new_entity.env
        if self.params:
            evaluated_params = {}
            for key, param in self.params.items():
                evaluated_params[key] = self.loader.eval_code_block(param, context, as_f_string=True)
            new_entity.params = evaluated_params
            context['params'] = new_entity.params
        return new_entity

    def rank_destinations(self, destinations, context):
        if self.rank:
            log.debug(f"Ranking destinations: {destinations} for entity: {self} using custom function")
            context['candidate_destinations'] = destinations
            return self.loader.eval_code_block(self.rank, context)
        else:
            # Sort destinations by priority
            log.debug(f"Ranking destinations: {destinations} for entity: {self} using default ranker")
            return sorted(destinations, key=lambda d: d.score(self), reverse=True)

    def score(self, entity):
        score = self.tags.score(entity.tags)
        log.debug(f"Destination: {entity} scored: {score}")
        return score


class EntityWithRules(Entity):

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
                log.exception(f"Could not load rule for entity: {self.__class__} with id: {self.id} and data: {rule}")
                raise
        return validated

    @classmethod
    def from_dict(cls: type, loader, entity_dict):
        return cls(
            loader=loader,
            id=entity_dict.get('id'),
            cores=entity_dict.get('cores'),
            mem=entity_dict.get('mem'),
            gpus=entity_dict.get('gpus'),
            env=entity_dict.get('env'),
            params=entity_dict.get('params'),
            tags=entity_dict.get('scheduling'),
            rank=entity_dict.get('rank'),
            inherits=entity_dict.get('inherits'),
            rules=entity_dict.get('rules')
        )

    def override(self, entity):
        new_entity = super().override(entity)
        new_entity.rules = copy.deepcopy(entity.rules)
        new_entity.rules.update(self.rules or {})
        for rule in self.rules.values():
            if entity.rules.get(rule.id):
                new_entity.rules[rule.id] = rule.inherit(entity.rules[rule.id])
        return new_entity

    def evaluate_early(self, context):
        new_entity = copy.deepcopy(self)
        for rule in self.rules.values():
            if rule.is_matching(context):
                rule = rule.evaluate_early(context)
                entity_id = new_entity.id
                new_entity.gpus = rule.gpus or self.gpus
                new_entity.cores = rule.cores or self.cores
                new_entity.mem = rule.mem or self.mem
                new_entity.id = f"{entity_id}, Rule: {rule.id}"
        return super(EntityWithRules, new_entity).evaluate_early(context)

    def evaluate_late(self, context):
        new_entity = copy.deepcopy(self)
        for rule in self.rules.values():
            if rule.is_matching(context):
                rule = rule.evaluate_late(context)
                new_entity = rule.inherit(new_entity)
                # restore already evaluated entity requirements
                new_entity.cores = self.cores
                new_entity.mem = self.mem
                new_entity.gpus = self.gpus
        return super(EntityWithRules, new_entity).evaluate_late(context)

    def __repr__(self):
        return super().__repr__() + f", rules={self.rules}"


class Tool(EntityWithRules):

    def __init__(self, loader, id=None, cores=None, mem=None, gpus=None,
                 env=None, params=None, tags=None, rank=None, inherits=None, rules=None):
        super().__init__(loader, id, cores, mem, gpus, env, params, tags, rank, inherits, rules)


class User(EntityWithRules):

    def __init__(self, loader, id=None, cores=None, mem=None, gpus=None,
                 env=None, params=None, tags=None, rank=None, inherits=None, rules=None):
        super().__init__(loader, id, cores, mem, gpus, env, params, tags, rank, inherits, rules)


class Role(EntityWithRules):

    def __init__(self, loader, id=None, cores=None, mem=None, gpus=None,
                 env=None, params=None, tags=None, rank=None, inherits=None, rules=None):
        super().__init__(loader, id, cores, mem, gpus, env, params, tags, rank, inherits, rules)


class Destination(EntityWithRules):

    def __init__(self, loader, id=None, cores=None, mem=None, gpus=None,
                 env=None, params=None, tags=None, inherits=None, rules=None):
        super().__init__(loader, id, cores, mem, gpus, env, params, tags, inherits, rules=rules)

    @staticmethod
    def from_dict(loader, entity_dict):
        return Destination(
            loader=loader,
            id=entity_dict.get('id'),
            cores=entity_dict.get('cores'),
            mem=entity_dict.get('mem'),
            gpus=entity_dict.get('gpus'),
            env=entity_dict.get('env'),
            params=entity_dict.get('params'),
            tags=entity_dict.get('scheduling'),
            inherits=entity_dict.get('inherits'),
            rules=entity_dict.get('rules')
        )


class Rule(Entity):

    rule_counter = 0

    def __init__(self, loader, id=None, cores=None, mem=None, gpus=None,
                 env=None, params=None, tags=None, inherits=None, match=None, execute=None, fail=None):
        if not id:
            Rule.rule_counter += 1
            id = f"vortex_rule_{Rule.rule_counter}"
        super().__init__(loader, id, cores, mem, gpus, env, params, tags, inherits=inherits)
        self.match = match
        self.execute = execute
        self.fail = fail
        if self.match:
            self.loader.compile_code_block(self.match)
        if self.execute:
            self.loader.compile_code_block(self.execute, exec_only=True)
        if self.fail:
            self.loader.compile_code_block(self.fail, as_f_string=True)

    @staticmethod
    def from_dict(loader, entity_dict):
        return Rule(
            loader=loader,
            id=entity_dict.get('id'),
            cores=entity_dict.get('cores'),
            mem=entity_dict.get('mem'),
            gpus=entity_dict.get('gpus'),
            env=entity_dict.get('env'),
            params=entity_dict.get('params'),
            tags=entity_dict.get('scheduling'),
            inherits=entity_dict.get('inherits'),
            # TODO: Remove deprecated match clause in future
            match=entity_dict.get('if') or entity_dict.get('match'),
            execute=entity_dict.get('execute'),
            fail=entity_dict.get('fail')
        )

    def override(self, entity):
        new_entity = super().override(entity)
        new_entity.match = self.match if self.match is not None else getattr(entity, 'match', None)
        new_entity.execute = self.execute if self.execute is not None else getattr(entity, 'execute', None)
        new_entity.fail = self.fail if self.fail is not None else getattr(entity, 'fail', None)
        return new_entity

    def __repr__(self):
        return super().__repr__() + f", if={self.match[:10] if self.match else ''}, " \
                                    f"execute={self.execute[:10] if self.execute else ''}, " \
                                    f"fail={self.fail[:10] if self.fail else ''}"

    def is_matching(self, context):
        if self.loader.eval_code_block(self.match, context):
            return True
        else:
            return False

    def evaluate_early(self, context):
        if self.fail:
            from galaxy.jobs.mapper import JobMappingException
            raise JobMappingException(
                self.loader.eval_code_block(self.fail, context, as_f_string=True))
        return self

    def evaluate_late(self, context):
        if self.execute:
            self.loader.eval_code_block(self.execute, context, exec_only=True)
        return self
