from __future__ import annotations

from enum import Enum
import logging
import copy

from galaxy import util as galaxy_util

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


class TryNextDestinationOrFail(Exception):
    # Try next destination, fail job if destination options exhausted
    pass


class TryNextDestinationOrWait(Exception):
    # Try next destination, raise JobNotReadyException if destination options exhausted
    pass


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
        assert type(self) is type(other)
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

    def to_dict(self) -> dict:
        result_dict = {
            'require': [tag.value for tag in self.tags if tag.tag_type == TagType.REQUIRE],
            'prefer': [tag.value for tag in self.tags if tag.tag_type == TagType.PREFER],
            'accept': [tag.value for tag in self.tags if tag.tag_type == TagType.ACCEPT],
            'reject': [tag.value for tag in self.tags if tag.tag_type == TagType.REJECT]
        }
        return result_dict


class Entity(object):

    merge_order = 0

    def __init__(self, loader, id=None, abstract=False, cores=None, mem=None, gpus=None, min_cores=None, min_mem=None,
                 min_gpus=None, max_cores=None, max_mem=None, max_gpus=None, env=None, params=None, resubmit=None,
                 tpv_tags=None, rank=None, inherits=None, context=None):
        self.loader = loader
        self.id = id
        self.abstract = galaxy_util.asbool(abstract)
        self.cores = cores
        self.mem = mem
        self.gpus = gpus
        self.min_cores = min_cores
        self.min_mem = min_mem
        self.min_gpus = min_gpus
        self.max_cores = max_cores
        self.max_mem = max_mem
        self.max_gpus = max_gpus
        self.env = self.convert_env(env)
        self.params = params
        self.resubmit = resubmit
        self.tpv_tags = TagSetManager.from_dict(tpv_tags or {})
        self.rank = rank
        self.inherits = inherits
        self.context = context
        self.validate()

    def __deepcopy__(self, memodict={}):
        # make sure we don't deepcopy the loader: https://github.com/galaxyproject/total-perspective-vortex/issues/53
        # xref: https://stackoverflow.com/a/15774013
        cls = self.__class__
        result = cls.__new__(cls)
        memodict[id(self)] = result
        for k, v in self.__dict__.items():
            if k == "loader":
                setattr(result, k, v)
            else:
                setattr(result, k, copy.deepcopy(v, memodict))
        return result

    def process_complex_property(self, prop, context, func, stringify=False):
        if isinstance(prop, str):
            return func(prop, context)
        elif isinstance(prop, dict):
            evaluated_props = {key: self.process_complex_property(childprop, context, func, stringify=stringify)
                               for key, childprop in prop.items()}
            return evaluated_props
        elif isinstance(prop, list):
            evaluated_props = [self.process_complex_property(childprop, context, func, stringify=stringify)
                               for childprop in prop]
            return evaluated_props
        else:
            return str(prop) if stringify else prop  # To handle special case of env vars provided as ints

    def compile_complex_property(self, prop):
        return self.process_complex_property(
            prop, None, lambda p, c: self.loader.compile_code_block(p, as_f_string=True))

    def evaluate_complex_property(self, prop, context, stringify=False):
        return self.process_complex_property(
            prop, context, lambda p, c: self.loader.eval_code_block(p, c, as_f_string=True), stringify=stringify)

    def convert_env(self, env):
        if isinstance(env, dict):
            env = [dict(name=k, value=v) for (k, v) in env.items()]
        return env

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
        if self.min_cores:
            self.loader.compile_code_block(self.min_cores)
        if self.min_mem:
            self.loader.compile_code_block(self.min_mem)
        if self.min_gpus:
            self.loader.compile_code_block(self.min_gpus)
        if self.max_cores:
            self.loader.compile_code_block(self.max_cores)
        if self.max_mem:
            self.loader.compile_code_block(self.max_mem)
        if self.max_gpus:
            self.loader.compile_code_block(self.max_gpus)
        if self.env:
            self.compile_complex_property(self.env)
        if self.params:
            self.compile_complex_property(self.params)
        if self.resubmit:
            self.compile_complex_property(self.resubmit)
        if self.rank:
            self.loader.compile_code_block(self.rank)

    def __repr__(self):
        return f"{self.__class__} id={self.id}, abstract={self.abstract}, cores={self.cores}, mem={self.mem}, " \
               f"gpus={self.gpus}, min_cores = {self.min_cores}, min_mem = {self.min_mem}, " \
               f"min_gpus = {self.min_gpus}, max_cores = {self.max_cores}, max_mem = {self.max_mem}, " \
               f"max_gpus = {self.max_gpus}, env={self.env}, params={self.params}, resubmit={self.resubmit}, " \
               f"tags={self.tpv_tags}, rank={self.rank[:10] if self.rank else ''}, inherits={self.inherits}, "\
               f"context={self.context}"

    def merge_env_list(self, original, replace):
        for i, original_elem in enumerate(original):
            for j, replace_elem in enumerate(replace):
                if (("name" in replace_elem and original_elem.get("name") == replace_elem["name"])
                        or original_elem == replace_elem):
                    original[i] = replace.pop(j)
                    break
        original.extend(replace)
        return original

    def override(self, entity):
        if entity.merge_order <= self.merge_order:
            # Use the broader class as a base when copying. Useful in particular for Rules
            new_entity = copy.copy(self)
        else:
            new_entity = copy.copy(entity)
        new_entity.id = self.id or entity.id
        new_entity.abstract = self.abstract and entity.abstract
        new_entity.cores = self.cores if self.cores is not None else entity.cores
        new_entity.mem = self.mem if self.mem is not None else entity.mem
        new_entity.gpus = self.gpus if self.gpus is not None else entity.gpus
        new_entity.min_cores = self.min_cores if self.min_cores is not None else entity.min_cores
        new_entity.min_mem = self.min_mem if self.min_mem is not None else entity.min_mem
        new_entity.min_gpus = self.min_gpus if self.min_gpus is not None else entity.min_gpus
        new_entity.max_cores = self.max_cores if self.max_cores is not None else entity.max_cores
        new_entity.max_mem = self.max_mem if self.max_mem is not None else entity.max_mem
        new_entity.max_gpus = self.max_gpus if self.max_gpus is not None else entity.max_gpus
        new_entity.env = self.merge_env_list(copy.deepcopy(entity.env) or [], copy.deepcopy(self.env) or [])
        new_entity.params = copy.copy(entity.params) or {}
        new_entity.params.update(self.params or {})
        new_entity.resubmit = copy.copy(entity.resubmit) or {}
        new_entity.resubmit.update(self.resubmit or {})
        new_entity.rank = self.rank if self.rank is not None else entity.rank
        new_entity.inherits = self.inherits if self.inherits is not None else entity.inherits
        new_entity.context = copy.copy(entity.context) or {}
        new_entity.context.update(self.context or {})
        return new_entity

    def inherit(self, entity):
        if entity:
            new_entity = self.override(entity)
            new_entity.tpv_tags = self.tpv_tags.inherit(entity.tpv_tags)
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

        The general hierarchy of entities in vortex is Destination > User > Role > Tool and therefore, these entity
        are usually merged as: destination.combine(user).combine(role).combine(tool), to produce a final set of tool
        requirements.

        The combined requirements can then be matched against the destination, through the match operation.

        :param entity:
        :return:
        """
        new_entity = self.override(entity)
        new_entity.id = f"{type(self).__name__}: {self.id}, {type(entity).__name__}: {entity.id}"
        new_entity.tpv_tags = entity.tpv_tags.combine(self.tpv_tags)
        return new_entity

    def evaluate_resources(self, context):
        new_entity = copy.deepcopy(self)
        context.update(self.context or {})
        if self.min_gpus is not None:
            new_entity.min_gpus = self.loader.eval_code_block(self.min_gpus, context)
            context['min_gpus'] = new_entity.min_gpus
        if self.min_cores is not None:
            new_entity.min_cores = self.loader.eval_code_block(self.min_cores, context)
            context['min_cores'] = new_entity.min_cores
        if self.min_mem is not None:
            new_entity.min_mem = self.loader.eval_code_block(self.min_mem, context)
            context['min_mem'] = new_entity.min_mem
        if self.max_gpus is not None:
            new_entity.max_gpus = self.loader.eval_code_block(self.max_gpus, context)
            context['max_gpus'] = new_entity.max_gpus
        if self.max_cores is not None:
            new_entity.max_cores = self.loader.eval_code_block(self.max_cores, context)
            context['max_cores'] = new_entity.max_cores
        if self.max_mem is not None:
            new_entity.max_mem = self.loader.eval_code_block(self.max_mem, context)
            context['max_mem'] = new_entity.max_mem
        if self.gpus is not None:
            new_entity.gpus = self.loader.eval_code_block(self.gpus, context)
            # clamp gpus
            new_entity.gpus = max(new_entity.min_gpus or 0, new_entity.gpus or 0)
            new_entity.gpus = min(new_entity.max_gpus, new_entity.gpus) if new_entity.max_gpus else new_entity.gpus
            context['gpus'] = new_entity.gpus
        if self.cores is not None:
            new_entity.cores = self.loader.eval_code_block(self.cores, context)
            # clamp cores
            new_entity.cores = max(new_entity.min_cores or 0, new_entity.cores or 0)
            new_entity.cores = min(new_entity.max_cores, new_entity.cores) if new_entity.max_cores else new_entity.cores
            context['cores'] = new_entity.cores
        if self.mem is not None:
            new_entity.mem = self.loader.eval_code_block(self.mem, context)
            # clamp mem
            new_entity.mem = max(new_entity.min_mem or 0, new_entity.mem or 0)
            new_entity.mem = min(new_entity.max_mem, new_entity.mem or 0) if new_entity.max_mem else new_entity.mem
            context['mem'] = new_entity.mem
        return new_entity

    def evaluate(self, context):
        """
        Evaluate expressions in entity properties that must be evaluated as late as possible, which is
        to say, after combining entity requirements. This includes env, params and resubmit, that rely on
        properties such as cores, mem and gpus after they are combined.
        :param context:
        :return:
        """
        new_entity = self.evaluate_resources(context)
        if self.env:
            new_entity.env = self.evaluate_complex_property(self.env, context, stringify=True)
            context['env'] = new_entity.env
        if self.params:
            new_entity.params = self.evaluate_complex_property(self.params, context)
            context['params'] = new_entity.params
        if self.resubmit:
            new_entity.resubmit = self.evaluate_complex_property(self.resubmit, context)
            context['resubmit'] = new_entity.resubmit
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


class EntityWithRules(Entity):

    merge_order = 1

    def __init__(self, loader, id=None, abstract=False, cores=None, mem=None, gpus=None, min_cores=None, min_mem=None,
                 min_gpus=None, max_cores=None, max_mem=None, max_gpus=None, env=None,
                 params=None, resubmit=None, tpv_tags=None, rank=None, inherits=None, context=None, rules=None):
        super().__init__(loader, id=id, abstract=abstract, cores=cores, mem=mem, gpus=gpus, min_cores=min_cores,
                         min_mem=min_mem, min_gpus=min_gpus, max_cores=max_cores, max_mem=max_mem, max_gpus=max_gpus,
                         env=env, params=params, resubmit=resubmit, tpv_tags=tpv_tags, rank=rank, inherits=inherits,
                         context=context)
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
            abstract=entity_dict.get('abstract'),
            cores=entity_dict.get('cores'),
            mem=entity_dict.get('mem'),
            gpus=entity_dict.get('gpus'),
            min_cores=entity_dict.get('min_cores'),
            min_mem=entity_dict.get('min_mem'),
            min_gpus=entity_dict.get('min_gpus'),
            max_cores=entity_dict.get('max_cores'),
            max_mem=entity_dict.get('max_mem'),
            max_gpus=entity_dict.get('max_gpus'),
            env=entity_dict.get('env'),
            params=entity_dict.get('params'),
            resubmit=entity_dict.get('resubmit'),
            tpv_tags=entity_dict.get('scheduling'),
            rank=entity_dict.get('rank'),
            inherits=entity_dict.get('inherits'),
            context=entity_dict.get('context'),
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

    def evaluate_rules(self, context):
        new_entity = copy.deepcopy(self)
        context.update(new_entity.context or {})
        for rule in self.rules.values():
            if rule.is_matching(context):
                rule = rule.evaluate(context)
                new_entity = rule.inherit(new_entity)
                new_entity.gpus = rule.gpus or new_entity.gpus
                new_entity.cores = rule.cores or new_entity.cores
                new_entity.mem = rule.mem or new_entity.mem
                new_entity.id = f"{new_entity.id}, Rule: {rule.id}"
                context.update({
                    'entity': new_entity
                })
        return new_entity

    def evaluate(self, context):
        new_entity = self.evaluate_rules(context)
        return super(EntityWithRules, new_entity).evaluate(context)

    def __repr__(self):
        return super().__repr__() + f", rules={self.rules}"


class Tool(EntityWithRules):

    merge_order = 2


class Role(EntityWithRules):

    merge_order = 3


class User(EntityWithRules):

    merge_order = 4


class Destination(EntityWithRules):

    merge_order = 5

    def __init__(self, loader, id=None, abstract=False, runner=None, dest_name=None, cores=None, mem=None, gpus=None,
                 min_cores=None, min_mem=None, min_gpus=None, max_cores=None, max_mem=None, max_gpus=None,
                 min_accepted_cores=None, min_accepted_mem=None, min_accepted_gpus=None,
                 max_accepted_cores=None, max_accepted_mem=None, max_accepted_gpus=None, env=None, params=None,
                 resubmit=None, tpv_dest_tags=None, inherits=None, context=None, rules=None, handler_tags=None):
        self.runner = runner
        self.dest_name = dest_name or id
        self.min_accepted_cores = min_accepted_cores
        self.min_accepted_mem = min_accepted_mem
        self.min_accepted_gpus = min_accepted_gpus
        self.max_accepted_cores = max_accepted_cores
        self.max_accepted_mem = max_accepted_mem
        self.max_accepted_gpus = max_accepted_gpus
        self.tpv_dest_tags = TagSetManager.from_dict(tpv_dest_tags or {})
        # Handler tags refer to Galaxy's job handler level tags
        self.handler_tags = handler_tags
        super().__init__(loader, id=id, abstract=abstract, cores=cores, mem=mem, gpus=gpus, min_cores=min_cores,
                         min_mem=min_mem, min_gpus=min_gpus, max_cores=max_cores, max_mem=max_mem, max_gpus=max_gpus,
                         env=env, params=params, resubmit=resubmit, tpv_tags=None, inherits=inherits, context=context,
                         rules=rules)

    @staticmethod
    def from_dict(loader, entity_dict):
        return Destination(
            loader=loader,
            id=entity_dict.get('id'),
            abstract=entity_dict.get('abstract'),
            runner=entity_dict.get('runner'),
            dest_name=entity_dict.get('destination_name_override'),
            cores=entity_dict.get('cores'),
            mem=entity_dict.get('mem'),
            gpus=entity_dict.get('gpus'),
            min_cores=entity_dict.get('min_cores'),
            min_mem=entity_dict.get('min_mem'),
            min_gpus=entity_dict.get('min_gpus'),
            max_cores=entity_dict.get('max_cores'),
            max_mem=entity_dict.get('max_mem'),
            max_gpus=entity_dict.get('max_gpus'),
            min_accepted_cores=entity_dict.get('min_accepted_cores'),
            min_accepted_mem=entity_dict.get('min_accepted_mem'),
            min_accepted_gpus=entity_dict.get('min_accepted_gpus'),
            max_accepted_cores=entity_dict.get('max_accepted_cores'),
            max_accepted_mem=entity_dict.get('max_accepted_mem'),
            max_accepted_gpus=entity_dict.get('max_accepted_gpus'),
            env=entity_dict.get('env'),
            params=entity_dict.get('params'),
            resubmit=entity_dict.get('resubmit'),
            tpv_dest_tags=entity_dict.get('scheduling'),
            inherits=entity_dict.get('inherits'),
            context=entity_dict.get('context'),
            rules=entity_dict.get('rules'),
            handler_tags=entity_dict.get('tags')
        )

    def to_dict(self):
        dest_dict = {
            'id': self.id,
            'abstract': self.abstract,
            'runner': self.runner,
            'destination_name_override': self.dest_name,
            'cores': self.cores,
            'mem': self.mem,
            'gpus': self.gpus,
            'min_cores': self.min_cores,
            'min_mem': self.min_mem,
            'min_gpus': self.min_gpus,
            'max_cores': self.max_cores,
            'max_mem': self.max_mem,
            'max_gpus': self.max_gpus,
            'min_accepted_cores': self.min_accepted_cores,
            'min_accepted_mem': self.min_accepted_mem,
            'min_accepted_gpus': self.min_accepted_gpus,
            'max_accepted_cores': self.max_accepted_cores,
            'max_accepted_mem': self.max_accepted_mem,
            'max_accepted_gpus': self.max_accepted_gpus,
            'env': self.env,
            'params': self.params,
            'resubmit': self.resubmit,
            'scheduling': self.tpv_dest_tags.to_dict(),
            'inherits': self.inherits,
            'context': self.context,
            'rules': self.rules,
            'tags': self.handler_tags
        }

        return dest_dict

    def __repr__(self):
        return f"runner={self.runner}, dest_name={self.dest_name}, min_accepted_cores={self.min_accepted_cores}, "\
               f"min_accepted_mem={self.min_accepted_mem}, min_accepted_gpus={self.min_accepted_gpus}, "\
               f"max_accepted_cores={self.max_accepted_cores}, max_accepted_mem={self.max_accepted_mem}, "\
               f"max_accepted_gpus={self.max_accepted_gpus}, tpv_dest_tags={self.tpv_dest_tags}, "\
               f"handler_tags={self.handler_tags}" + super().__repr__()

    def override(self, entity):
        new_entity = super().override(entity)
        new_entity.runner = self.runner if self.runner is not None else getattr(entity, 'runner', None)
        new_entity.dest_name = self.dest_name if self.dest_name is not None else getattr(entity, 'dest_name', None)
        new_entity.min_accepted_cores = (self.min_accepted_cores if self.min_accepted_cores is not None
                                         else getattr(entity, 'min_accepted_cores', None))
        new_entity.min_accepted_mem = (self.min_accepted_mem if self.min_accepted_mem is not None
                                       else getattr(entity, 'min_accepted_mem', None))
        new_entity.min_accepted_gpus = (self.min_accepted_gpus if self.min_accepted_gpus is not None
                                        else getattr(entity, 'min_accepted_gpus', None))
        new_entity.max_accepted_cores = (self.max_accepted_cores if self.max_accepted_cores is not None
                                         else getattr(entity, 'max_accepted_cores', None))
        new_entity.max_accepted_mem = (self.max_accepted_mem if self.max_accepted_mem is not None
                                       else getattr(entity, 'max_accepted_mem', None))
        new_entity.max_accepted_gpus = (self.max_accepted_gpus if self.max_accepted_gpus is not None
                                        else getattr(entity, 'max_accepted_gpus', None))
        new_entity.handler_tags = self.handler_tags or getattr(entity, 'handler_tags', None)
        return new_entity

    def validate(self):
        """
        Validates each code block and makes sure the code can be compiled.
        This process also results in the compiled code being cached by the loader,
        so that future evaluations are faster.
        """
        super().validate()
        if self.dest_name:
            self.loader.compile_code_block(self.dest_name, as_f_string=True)
        if self.handler_tags:
            self.compile_complex_property(self.handler_tags)

    def evaluate(self, context):
        new_entity = super(Destination, self).evaluate(context)
        if self.dest_name is not None:
            new_entity.dest_name = self.loader.eval_code_block(self.dest_name, context, as_f_string=True)
            context['dest_name'] = new_entity.dest_name
        if self.handler_tags is not None:
            new_entity.handler_tags = self.evaluate_complex_property(self.handler_tags, context)
            context['handler_tags'] = new_entity.handler_tags
        return new_entity

    def inherit(self, entity):
        new_entity = super().inherit(entity)
        if entity:
            new_entity.tpv_dest_tags = self.tpv_dest_tags.inherit(entity.tpv_dest_tags)
        return new_entity

    def matches(self, entity, context):
        """
        The match operation checks whether

        a. The destination is not abstract.
        b. The cores, mem and gpu defined on the destination are sufficient to fulfill the cores, mem and gpus
           requested by the entity. If not defined, it is considered a match.
        c. all of the require tags in an entity are present in the destination entity, and none of the reject tags in
           the first entity are present in the second entity.

        This is used to check compatibility of a final set of combined tool requirements with its destination.

        :param destination:
        :return:
        """
        if self.abstract:
            return False
        if self.max_accepted_cores is not None and entity.cores is not None and self.max_accepted_cores < entity.cores:
            return False
        if self.max_accepted_mem is not None and entity.mem is not None and self.max_accepted_mem < entity.mem:
            return False
        if self.max_accepted_gpus is not None and entity.gpus is not None and self.max_accepted_gpus < entity.gpus:
            return False
        if self.min_accepted_cores is not None and entity.cores is not None and self.min_accepted_cores > entity.cores:
            return False
        if self.min_accepted_mem is not None and entity.mem is not None and self.min_accepted_mem > entity.mem:
            return False
        if self.min_accepted_gpus is not None and entity.gpus is not None and self.min_accepted_gpus > entity.gpus:
            return False
        return entity.tpv_tags.match(self.tpv_dest_tags or {})

    def score(self, entity):
        """
        Rank this destination against an entity based on how well the tags match

        :param entity:
        :return:
        """
        score = self.tpv_dest_tags.score(entity.tpv_tags)
        log.debug(f"Destination: {entity} scored: {score}")
        return score


class Rule(Entity):

    rule_counter = 0
    merge_order = 0

    def __init__(self, loader, id=None, cores=None, mem=None, gpus=None, min_cores=None, min_mem=None, min_gpus=None,
                 max_cores=None, max_mem=None, max_gpus=None, env=None, params=None, resubmit=None,
                 tpv_tags=None, inherits=None, context=None, match=None, execute=None, fail=None):
        if not id:
            Rule.rule_counter += 1
            id = f"tpv_rule_{Rule.rule_counter}"
        super().__init__(loader, id=id, abstract=False, cores=cores, mem=mem, gpus=gpus, min_cores=min_cores,
                         min_mem=min_mem, min_gpus=min_gpus, max_cores=max_cores, max_mem=max_mem, max_gpus=max_gpus,
                         env=env, params=params, resubmit=resubmit, tpv_tags=tpv_tags, context=context,
                         inherits=inherits)
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
            min_cores=entity_dict.get('min_cores'),
            min_mem=entity_dict.get('min_mem'),
            min_gpus=entity_dict.get('min_gpus'),
            max_cores=entity_dict.get('max_cores'),
            max_mem=entity_dict.get('max_mem'),
            max_gpus=entity_dict.get('max_gpus'),
            env=entity_dict.get('env'),
            params=entity_dict.get('params'),
            resubmit=entity_dict.get('resubmit'),
            tpv_tags=entity_dict.get('scheduling'),
            inherits=entity_dict.get('inherits'),
            context=entity_dict.get('context'),
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

    def evaluate(self, context):
        if self.fail:
            from galaxy.jobs.mapper import JobMappingException
            raise JobMappingException(
                self.loader.eval_code_block(self.fail, context, as_f_string=True))
        if self.execute:
            self.loader.eval_code_block(self.execute, context, exec_only=True)
            # return any changes made to the entity
            return context['entity']
        return self
