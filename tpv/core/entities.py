import copy
import itertools
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from enum import IntEnum
from typing import (
    Annotated,
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
    cast,
)

from galaxy import util as galaxy_util
from pydantic import BaseModel, Field, model_validator
from pydantic.json_schema import SkipJsonSchema

# xref: https://github.com/python/mypy/issues/12664
from ruamel.yaml.comments import CommentedMap
from typing_extensions import Self

from .evaluator import TPVCodeEvaluator

log = logging.getLogger(__name__)


NOQA_RE = re.compile(r"#\s*noqa:?\s*([A-Z0-9, ]+)?")


class TryNextDestinationOrFail(Exception):
    # Try next destination, fail job if destination options exhausted
    pass


class TryNextDestinationOrWait(Exception):
    # Try next destination, raise JobNotReadyException if destination options exhausted
    pass


@dataclass
class TPVFieldMetadata:
    # complex properties are always evaluated as f-strings
    complex_property: bool = False
    return_type: Union[type, None] = None
    eval_as_f_string: bool = False


FieldCopierType = Callable[["Entity", "Entity", str], Any]


def default_field_copier(entity1: "Entity", entity2: "Entity", property_name: str) -> Any:
    # if property_name in entity1.model_fields_set
    return (
        getattr(
            entity1,
            property_name,
        )
        if getattr(entity1, property_name, None) is not None
        else getattr(entity2, property_name, None)
    )


def default_dict_copier(entity1: "Entity", entity2: "Entity", property_name: str) -> Any:
    new_dict = copy.deepcopy(getattr(entity2, property_name)) or {}
    new_dict.update(copy.deepcopy(getattr(entity1, property_name)) or {})
    return new_dict


class TagType(IntEnum):
    REQUIRE = 2
    PREFER = 1
    ACCEPT = 0
    REJECT = -1


@dataclass(frozen=True)
class Tag:
    value: str
    tag_type: TagType


class IncompatibleTagsException(Exception):
    def __init__(self, first_set: "SchedulingTags", second_set: "SchedulingTags"):
        super().__init__(
            "Cannot combine tag sets because require and reject tags mismatch. First"
            f" tag set requires: {first_set.require} and rejects: {first_set.reject}."
            f" Second tag set requires: {second_set.require} and rejects:"
            f" {second_set.reject}."
        )


class SchedulingTags(BaseModel):
    require: Optional[List[str]] = Field(default_factory=lambda: list())
    prefer: Optional[List[str]] = Field(default_factory=lambda: list())
    accept: Optional[List[str]] = Field(default_factory=lambda: list())
    reject: Optional[List[str]] = Field(default_factory=lambda: list())

    class Config:
        extra = "allow"

    @model_validator(mode="after")
    def check_duplicates(self) -> Self:
        tag_occurrences = defaultdict(list)

        # Track tag occurrences within each category and across categories
        for tag_type in TagType:
            field = tag_type.name.lower()
            tags = getattr(self, field, []) or []
            for tag in tags:
                tag_occurrences[tag].append(field)

        # Identify duplicates
        duplicates = {tag: fields for tag, fields in tag_occurrences.items() if len(fields) > 1}

        # Build the detailed error message
        if duplicates:
            details = "; ".join([f"'{tag}' in {fields}" for tag, fields in duplicates.items()])
            raise ValueError(f"Duplicate tags found: {details}")

        return self

    @property
    def tags(self) -> Iterable[Tag]:
        return itertools.chain(
            (Tag(value=tag, tag_type=TagType.REQUIRE) for tag in self.require or []),
            (Tag(value=tag, tag_type=TagType.PREFER) for tag in self.prefer or []),
            (Tag(value=tag, tag_type=TagType.ACCEPT) for tag in self.accept or []),
            (Tag(value=tag, tag_type=TagType.REJECT) for tag in self.reject or []),
        )

    def all_tag_values(self) -> Iterable[str]:
        return itertools.chain(self.require or [], self.prefer or [], self.accept or [], self.reject or [])

    def filter(
        self,
        tag_type: Optional[Union[TagType, list[TagType]]] = None,
        tag_value: Optional[str] = None,
    ) -> Iterable[Tag]:
        filtered = self.tags
        if tag_type:
            if isinstance(tag_type, TagType):
                filtered = (tag for tag in filtered if tag.tag_type == tag_type)
            else:
                filtered = (tag for tag in filtered if tag.tag_type in tag_type)
        if tag_value:
            filtered = (tag for tag in filtered if tag.value == tag_value)
        return filtered

    def add_tag_override(self, tag_type: TagType, tag_value: str) -> None:
        # Remove tag from all categories
        for field in TagType:
            field_name = field.name.lower()
            if tag_value in (getattr(self, field_name) or []):
                getattr(self, field_name).remove(tag_value)

        # Add tag to the specified category
        tag_field = tag_type.name.lower()
        current_tags = getattr(self, tag_field, []) or []
        setattr(self, tag_field, current_tags + [tag_value])

    def inherit(self, other: "SchedulingTags") -> "SchedulingTags":
        # Create new lists of tags that combine self and other
        new_tags = copy.deepcopy(other)
        for tag_type in [
            TagType.ACCEPT,
            TagType.PREFER,
            TagType.REQUIRE,
            TagType.REJECT,
        ]:
            for tag in getattr(self, tag_type.name.lower()) or []:
                new_tags.add_tag_override(tag_type, tag)
        return new_tags

    def can_combine(self, other: "SchedulingTags") -> bool:
        self_required = set(self.require or [])
        other_required = set(other.require or [])
        self_rejected = set(self.reject or [])
        other_rejected = set(other.reject or [])

        if self_required.intersection(other_rejected) or self_rejected.intersection(other_required):
            return False
        return True

    def combine(self, other: "SchedulingTags") -> "SchedulingTags":
        if not self.can_combine(other):
            raise IncompatibleTagsException(self, other)

        new_tags = SchedulingTags()

        # Add tags in the specific precedence order
        for tag_type in [
            TagType.ACCEPT,
            TagType.PREFER,
            TagType.REQUIRE,
            TagType.REJECT,
        ]:
            for tag in getattr(other, tag_type.name.lower()) or []:
                new_tags.add_tag_override(tag_type, tag)
            for tag in getattr(self, tag_type.name.lower()) or []:
                new_tags.add_tag_override(tag_type, tag)

        return new_tags

    def match(self, other: "SchedulingTags") -> bool:
        self_required = set(self.require or [])
        other_required = set(other.require or [])
        self_rejected = set(self.reject or [])
        other_rejected = set(other.reject or [])

        return (
            self_required.issubset(other.all_tag_values())
            and other_required.issubset(self.all_tag_values())
            and not self_rejected.intersection(other.all_tag_values())
            and not other_rejected.intersection(self.all_tag_values())
        )

    def score(self, other: "SchedulingTags") -> int:
        return (
            sum(
                int(tag.tag_type) * int(o.tag_type)
                for tag in self.filter()
                for o in other.filter()
                if tag.value == o.value
            )
            # penalize tags that don't exist in the other
            - sum(int(tag.tag_type) for tag in self.tags if tag.value not in other.all_tag_values())
        )


class Entity(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        extra = "allow"

    merge_order: ClassVar[int] = 0
    # assign a dummy value for now, so pydantic does not treat this as a required field
    # it'll be set in the constructor later
    id: str = ""
    abstract: Optional[bool] = False
    inherits: Optional[str] = None
    cores: Annotated[Optional[Union[int, float, str]], TPVFieldMetadata()] = None
    mem: Annotated[Optional[Union[int, float, str]], TPVFieldMetadata()] = None
    gpus: Annotated[Optional[Union[int, str]], TPVFieldMetadata()] = None
    min_cores: Annotated[Optional[Union[int, float, str]], TPVFieldMetadata()] = None
    min_mem: Annotated[Optional[Union[int, float, str]], TPVFieldMetadata()] = None
    min_gpus: Annotated[Optional[Union[int, str]], TPVFieldMetadata()] = None
    max_cores: Annotated[Optional[Union[int, float, str]], TPVFieldMetadata()] = None
    max_mem: Annotated[Optional[Union[int, float, str]], TPVFieldMetadata()] = None
    max_gpus: Annotated[Optional[Union[int, str]], TPVFieldMetadata()] = None
    env: Annotated[Optional[List[Dict[str, str]]], TPVFieldMetadata(complex_property=True)] = None
    params: Annotated[Optional[Dict[str, Any]], TPVFieldMetadata(complex_property=True)] = None
    resubmit: Annotated[Dict[str, str], TPVFieldMetadata(complex_property=True)] = Field(default_factory=lambda: dict())
    rank: Annotated[Optional[str], TPVFieldMetadata(return_type="List[Destination]")] = None
    context: Optional[Dict[str, Any]] = Field(default_factory=lambda: dict())
    # evaluator is always assigned, so ignore the warning about default being None
    evaluator: SkipJsonSchema[TPVCodeEvaluator] = Field(exclude=True, default=cast(TPVCodeEvaluator, None))
    tpv_tags: SchedulingTags = Field(alias="scheduling", default_factory=SchedulingTags)
    no_qa_codes: SkipJsonSchema[List[str]] = Field(default_factory=lambda: list())

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.propagate_parent_properties(id=self.id, evaluator=self.evaluator)

    def propagate_parent_properties(self, id: str, evaluator: TPVCodeEvaluator) -> None:
        self.id = id
        self.evaluator = evaluator
        if evaluator:
            self.precompile_properties(evaluator)

    def precompile_properties(self, evaluator: TPVCodeEvaluator) -> None:
        # compile properties and check for errors
        if evaluator:
            for field_name, field in self.__class__.model_fields.items():
                value = getattr(self, field_name)
                if field.metadata and field.metadata[0]:
                    prop = field.metadata[0]
                    if isinstance(prop, TPVFieldMetadata):
                        if prop.complex_property:
                            evaluator.compile_complex_property(value)
                        else:
                            evaluator.compile_code_block(value)

    def __deepcopy__(self, memo: Union[dict[int, Any], None] = None) -> Self:
        # satisfy mypy by ensuring memo is never None
        if memo is None:
            memo = {}  # pragma: no cover
        # make sure we don't deepcopy the evaluator: https://github.com/galaxyproject/total-perspective-vortex/issues/53
        # xref: https://stackoverflow.com/a/68746763/10971151
        memo[id(self.evaluator)] = self.evaluator
        return super().__deepcopy__(memo)

    @staticmethod
    def convert_env(
        env: Optional[List[Dict[str, str]]],
    ) -> Optional[List[Dict[str, str]]]:
        if isinstance(env, dict):
            env = [dict(name=k, value=str(v)) for (k, v) in env.items()]
        return env

    @model_validator(mode="before")
    @classmethod
    def preprocess(cls, values: dict[str, Any]) -> dict[str, Any]:
        if values:
            values["abstract"] = galaxy_util.asbool(values.get("abstract", False))  # type: ignore[no-untyped-call]
            values["env"] = Entity.convert_env(values.get("env"))
        return values

    @staticmethod
    def merge_env_list(original: list[dict[str, str]], replace: list[dict[str, str]]) -> List[Dict[str, str]]:
        for i, original_elem in enumerate(original):
            for j, replace_elem in enumerate(replace):
                if (
                    "name" in replace_elem and original_elem.get("name") == replace_elem["name"]
                ) or original_elem == replace_elem:
                    original[i] = replace.pop(j)
                    break
        original.extend(replace)
        return original

    @staticmethod
    def override_single_property(
        entity: "Entity",
        entity1: "Entity",
        entity2: "Entity",
        property_name: str,
        field_copier: FieldCopierType = default_field_copier,
    ) -> None:
        setattr(entity, property_name, field_copier(entity1, entity2, property_name))

    def override(self, entity: Self) -> Self:
        if entity.merge_order <= self.merge_order:
            # Use the broader class as a base when copying. Useful in particular for Rules
            new_entity = self.model_copy()
        else:
            new_entity = entity.model_copy()
        self.override_single_property(new_entity, self, entity, "id")
        self.override_single_property(new_entity, self, entity, "abstract")
        self.override_single_property(new_entity, self, entity, "cores")
        self.override_single_property(new_entity, self, entity, "mem")
        self.override_single_property(new_entity, self, entity, "gpus")
        self.override_single_property(new_entity, self, entity, "min_cores")
        self.override_single_property(new_entity, self, entity, "min_mem")
        self.override_single_property(new_entity, self, entity, "min_gpus")
        self.override_single_property(new_entity, self, entity, "max_cores")
        self.override_single_property(new_entity, self, entity, "max_mem")
        self.override_single_property(new_entity, self, entity, "max_gpus")
        self.override_single_property(new_entity, self, entity, "max_gpus")
        self.override_single_property(
            new_entity,
            self,
            entity,
            "env",
            field_copier=lambda e1, e2, p: self.merge_env_list(
                copy.deepcopy(entity.env or []),
                copy.deepcopy(self.env or []),
            ),
        )
        self.override_single_property(new_entity, self, entity, "params", field_copier=default_dict_copier)
        self.override_single_property(new_entity, self, entity, "resubmit", field_copier=default_dict_copier)
        self.override_single_property(new_entity, self, entity, "rank")
        self.override_single_property(new_entity, self, entity, "inherits")
        self.override_single_property(new_entity, self, entity, "context", field_copier=default_dict_copier)
        return new_entity

    def inherit(self, entity: Self) -> Self:
        if entity:
            new_entity = self.override(entity)
            new_entity.tpv_tags = self.tpv_tags.inherit(entity.tpv_tags)
            return new_entity
        else:
            return copy.deepcopy(self)

    def combine(self, entity: Self) -> Self:
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

    def evaluate_resources(self, context: Dict[str, Any]) -> Self:
        new_entity = copy.deepcopy(self)
        context.update(self.context or {})
        if self.min_gpus is not None:
            new_entity.min_gpus = self.evaluator.eval_code_block(str(self.min_gpus), context)
            context["min_gpus"] = new_entity.min_gpus
        if self.min_cores is not None:
            new_entity.min_cores = self.evaluator.eval_code_block(str(self.min_cores), context)
            context["min_cores"] = new_entity.min_cores
        if self.min_mem is not None:
            new_entity.min_mem = self.evaluator.eval_code_block(str(self.min_mem), context)
            context["min_mem"] = new_entity.min_mem
        if self.max_gpus is not None:
            new_entity.max_gpus = self.evaluator.eval_code_block(str(self.max_gpus), context)
            context["max_gpus"] = new_entity.max_gpus
        if self.max_cores is not None:
            new_entity.max_cores = self.evaluator.eval_code_block(str(self.max_cores), context)
            context["max_cores"] = new_entity.max_cores
        if self.max_mem is not None:
            new_entity.max_mem = self.evaluator.eval_code_block(str(self.max_mem), context)
            context["max_mem"] = new_entity.max_mem
        if self.gpus is not None:
            new_entity.gpus = self.evaluator.eval_code_block(str(self.gpus), context)
            # clamp gpus
            new_entity.gpus = (
                max(new_entity.min_gpus or 0, new_entity.gpus or 0) if new_entity.min_gpus else new_entity.gpus
            )
            new_entity.gpus = (
                min(new_entity.max_gpus or 0, new_entity.gpus or 0) if new_entity.max_gpus else new_entity.gpus
            )
            context["gpus"] = new_entity.gpus
        if self.cores is not None:
            new_entity.cores = self.evaluator.eval_code_block(str(self.cores), context)
            # clamp cores
            new_entity.cores = (
                max(new_entity.min_cores or 0, new_entity.cores or 0) if new_entity.min_cores else new_entity.cores
            )
            new_entity.cores = (
                min(new_entity.max_cores or 0, new_entity.cores or 0) if new_entity.max_cores else new_entity.cores
            )
            context["cores"] = new_entity.cores
        if self.mem is not None:
            new_entity.mem = self.evaluator.eval_code_block(str(self.mem), context)
            # clamp mem
            new_entity.mem = max(new_entity.min_mem or 0, new_entity.mem or 0) if new_entity.min_mem else new_entity.mem
            new_entity.mem = min(new_entity.max_mem or 0, new_entity.mem or 0) if new_entity.max_mem else new_entity.mem
            context["mem"] = new_entity.mem
        return new_entity

    def evaluate(self, context: Dict[str, Any]) -> Self:
        """
        Evaluate expressions in entity properties that must be evaluated as late as possible, which is
        to say, after combining entity requirements. This includes env, params and resubmit, that rely on
        properties such as cores, mem and gpus after they are combined.
        :param context:
        :return:
        """
        new_entity = self.evaluate_resources(context)
        if self.env:
            new_entity.env = self.evaluator.evaluate_complex_property(self.env, context)
            context["env"] = new_entity.env
        if self.params:
            new_entity.params = self.evaluator.evaluate_complex_property(self.params, context)
            context["params"] = new_entity.params
        if self.resubmit:
            new_entity.resubmit = self.evaluator.evaluate_complex_property(self.resubmit, context)
            context["resubmit"] = new_entity.resubmit
        return new_entity

    def rank_destinations(self, destinations: List["Destination"], context: Dict[str, Any]) -> List["Destination"]:
        if self.rank:
            log.debug(f"Ranking destinations: {destinations} for entity: {self} using custom" " function")
            context["candidate_destinations"] = destinations
            return cast(List["Destination"], self.evaluator.eval_code_block(self.rank, context))
        else:
            # Sort destinations by priority
            log.debug(f"Ranking destinations: {destinations} for entity: {self} using default" " ranker")
            return sorted(destinations, key=lambda d: d.score(self), reverse=True)

    def should_skip_qa(self, code: str) -> bool:
        return "noqa" in self.no_qa_codes or code in self.no_qa_codes

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        # Ensure by_alias is set to True to use the field aliases during serialization
        kwargs.setdefault("by_alias", True)
        return super().model_dump(**kwargs)

    def dict(self, **kwargs: Any) -> Dict[str, Any]:
        # by_alias is set to True to use the field aliases during serialization
        kwargs.setdefault("by_alias", True)
        return super().dict(**kwargs)


class Rule(Entity):
    rule_counter: ClassVar[int] = 0
    id: str = Field(default_factory=lambda: Rule.set_default_id())
    if_condition: Annotated[Union[str, bool], TPVFieldMetadata()] = Field(alias="if")
    execute: Annotated[Optional[str], TPVFieldMetadata(return_type=type(None))] = None
    fail: Annotated[Optional[str], TPVFieldMetadata(eval_as_f_string=True)] = None

    @classmethod
    def set_default_id(cls) -> str:
        cls.rule_counter += 1
        return f"tpv_rule_{cls.rule_counter}"

    def override(self, entity: Self) -> Self:
        new_entity = super().override(entity)
        if isinstance(entity, Rule):
            self.override_single_property(new_entity, self, entity, "if_condition")
            self.override_single_property(new_entity, self, entity, "execute")
            self.override_single_property(new_entity, self, entity, "fail")
        return new_entity

    def is_matching(self, context: Dict[str, Any]) -> bool:
        if self.evaluator.eval_code_block(str(self.if_condition), context):
            return True
        else:
            return False

    def evaluate(self, context: Dict[str, Any]) -> Self:
        if self.fail:
            from galaxy.jobs.mapper import JobMappingException

            raise JobMappingException(
                self.evaluator.eval_code_block(self.fail, context, as_f_string=True)
            )  # type: ignore[no-untyped-call]
        if self.execute:
            self.evaluator.eval_code_block(self.execute, context, exec_only=True)
            # return any changes made to the entity
            return cast(Self, context["entity"])
        return self


class EntityWithRules(Entity):
    merge_order: ClassVar[int] = 1
    rules: Dict[str, Rule] = Field(default_factory=lambda: dict())

    def propagate_parent_properties(self, id: str, evaluator: TPVCodeEvaluator) -> None:
        super().propagate_parent_properties(id=id, evaluator=evaluator)
        for rule in self.rules.values():
            rule.evaluator = evaluator

    @model_validator(mode="before")
    @classmethod
    def deserialize_rules(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if "rules" in values and isinstance(values["rules"], list):
            rules = (Rule(**r) for r in values["rules"])
            values["rules"] = {rule.id: rule for rule in rules}
        return values

    def override(self, entity: Self) -> Self:
        new_entity = super().override(entity)
        new_entity.rules = copy.deepcopy(entity.rules)
        new_entity.rules.update(self.rules or {})
        for rule in self.rules.values():
            if entity.rules.get(rule.id):
                new_entity.rules[rule.id] = rule.inherit(entity.rules[rule.id])
        return new_entity

    def evaluate_rules(self, context: Dict[str, Any]) -> Self:
        new_entity = copy.deepcopy(self)
        context.update(new_entity.context or {})
        for rule in self.rules.values():
            if rule.is_matching(context):
                rule = rule.evaluate(context)
                new_entity = cast(Self, rule.inherit(cast(Rule, new_entity)))
                new_entity.gpus = rule.gpus or new_entity.gpus
                new_entity.cores = rule.cores or new_entity.cores
                new_entity.mem = rule.mem or new_entity.mem
                new_entity.id = f"{new_entity.id}, Rule: {rule.id}"
                context.update({"entity": new_entity})
        return new_entity

    def evaluate(self, context: Dict[str, Any]) -> Self:
        new_entity = self.evaluate_rules(context)
        return super(EntityWithRules, new_entity).evaluate(context)


class Tool(EntityWithRules):
    merge_order: ClassVar[int] = 2


class Role(EntityWithRules):
    merge_order: ClassVar[int] = 3


class User(EntityWithRules):
    merge_order: ClassVar[int] = 4


class Destination(EntityWithRules):
    merge_order: ClassVar[int] = 5
    runner: Optional[str] = None
    max_accepted_cores: Optional[Union[int, float]] = None
    max_accepted_mem: Optional[Union[int, float]] = None
    max_accepted_gpus: Optional[int] = None
    min_accepted_cores: Optional[Union[int, float]] = None
    min_accepted_mem: Optional[Union[int, float]] = None
    min_accepted_gpus: Optional[int] = None
    dest_name: Optional[str] = Field(alias="destination_name_override", default=None)
    # tpv_tags track what tags the entity being scheduled requested, while tpv_dest_tags track what the destination
    # supports. When serializing a Destination, we don't need tpv_tags, only tpv_dest_tags.
    tpv_tags: SkipJsonSchema[SchedulingTags] = Field(exclude=True, default_factory=SchedulingTags)
    tpv_dest_tags: SchedulingTags = Field(alias="scheduling", default_factory=SchedulingTags)
    handler_tags: Annotated[List[str], TPVFieldMetadata(complex_property=True)] = Field(
        alias="tags", default_factory=lambda: list()
    )

    def propagate_parent_properties(self, id: str, evaluator: TPVCodeEvaluator) -> None:
        super().propagate_parent_properties(id=id, evaluator=evaluator)
        self.dest_name = self.dest_name or self.id

    def override(self, entity: Self) -> Self:
        new_entity = super().override(entity)
        self.override_single_property(new_entity, self, entity, "runner")
        self.override_single_property(new_entity, self, entity, "dest_name")
        self.override_single_property(new_entity, self, entity, "min_accepted_cores")
        self.override_single_property(new_entity, self, entity, "min_accepted_mem")
        self.override_single_property(new_entity, self, entity, "min_accepted_gpus")
        self.override_single_property(new_entity, self, entity, "max_accepted_cores")
        self.override_single_property(new_entity, self, entity, "max_accepted_mem")
        self.override_single_property(new_entity, self, entity, "max_accepted_gpus")
        self.override_single_property(new_entity, self, entity, "handler_tags")
        return new_entity

    def evaluate(self, context: Dict[str, Any]) -> Self:
        new_entity = super(Destination, self).evaluate(context)
        if self.dest_name is not None:
            new_entity.dest_name = self.evaluator.eval_code_block(self.dest_name, context, as_f_string=True)
            context["dest_name"] = new_entity.dest_name
        if self.handler_tags is not None:
            new_entity.handler_tags = self.evaluator.evaluate_complex_property(self.handler_tags, context)
            context["handler_tags"] = new_entity.handler_tags
        return new_entity

    def inherit(self, entity: Self) -> Self:
        new_entity = super().inherit(entity)
        if entity:
            new_entity.tpv_dest_tags = self.tpv_dest_tags.inherit(entity.tpv_dest_tags)
        return new_entity

    def matches(self, entity: Entity, context: Dict[str, Any]) -> bool:
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
        if (
            self.max_accepted_cores is not None
            and entity.cores is not None
            and self.max_accepted_cores < float(entity.cores)
        ):
            return False
        if self.max_accepted_mem is not None and entity.mem is not None and self.max_accepted_mem < float(entity.mem):
            return False
        if (
            self.max_accepted_gpus is not None
            and entity.gpus is not None
            and self.max_accepted_gpus < float(entity.gpus)
        ):
            return False
        if (
            self.min_accepted_cores is not None
            and entity.cores is not None
            and self.min_accepted_cores > float(entity.cores)
        ):
            return False
        if self.min_accepted_mem is not None and entity.mem is not None and self.min_accepted_mem > float(entity.mem):
            return False
        if (
            self.min_accepted_gpus is not None
            and entity.gpus is not None
            and self.min_accepted_gpus > float(entity.gpus)
        ):
            return False
        return entity.tpv_tags.match(self.tpv_dest_tags)

    def score(self, entity: Entity) -> int:
        """
        Rank this destination against an entity based on how well the tags match

        :param entity:
        :return:
        """
        score = self.tpv_dest_tags.score(entity.tpv_tags)
        log.debug(f"Destination: {entity} scored: {score}")
        return score


class GlobalConfig(BaseModel):
    class Config:
        extra = "allow"

    default_inherits: Optional[str] = None
    context: Dict[str, Any] = Field(default_factory=lambda: dict())


class TPVConfig(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        extra = "allow"

    global_config: GlobalConfig = Field(alias="global", default_factory=GlobalConfig)
    evaluator: SkipJsonSchema[Optional[TPVCodeEvaluator]] = Field(exclude=True, default=None)
    tools: Dict[str, Tool] = Field(default_factory=lambda: dict())
    users: Dict[str, User] = Field(default_factory=lambda: dict())
    roles: Dict[str, Role] = Field(default_factory=lambda: dict())
    destinations: Dict[str, Destination] = Field(default_factory=lambda: dict())

    @model_validator(mode="after")
    def propagate_parent_properties(self) -> Self:
        if self.evaluator:
            for id, tool in self.tools.items():
                tool.propagate_parent_properties(id=id, evaluator=self.evaluator)
            for id, user in self.users.items():
                user.propagate_parent_properties(id=id, evaluator=self.evaluator)
            for id, role in self.roles.items():
                role.propagate_parent_properties(id=id, evaluator=self.evaluator)
            for id, destination in self.destinations.items():
                destination.propagate_parent_properties(id=id, evaluator=self.evaluator)
        return self

    @model_validator(mode="before")
    @classmethod
    def gather_comments(cls, data: Any) -> Any:
        """
        This runs before Pydantic constructs TPVConfig.
        We have one chance to transform 'data' if it's a CommentedMap,
        recursively attach comments to sub-dicts, and return the updated dict.
        """
        return TPVConfig.recursively_extract_comments(data)

    @staticmethod
    def get_noqa_codes(entity_comments: List[str]) -> Set[str]:
        for comment in entity_comments:
            match = NOQA_RE.match(comment)
            if match:
                codes = match.group(1)
                # Return a set of codes or None if `# noqa` with no codes
                return set(code.strip() for code in codes.split(",")) if codes else set(("noqa",))
        return set()

    @staticmethod
    def recursively_extract_comments(cm: CommentedMap) -> Any:
        """
        Recursively walk a ruamel.yaml CommentedMap, extracting comments and
        placing them under a special key (e.g., "no_qa_codes") for each item.
        Returns a dict that Pydantic can parse.
        """
        if not isinstance(cm, CommentedMap):
            # Base case: if it's a scalar or list or normal dict, just return it
            return cm

        # We'll build a new dict with the same keys, plus "no_qa_codes" if needed
        new_dict = {}
        for key, value in cm.items():
            # In ruamel.yaml, comments above or inline with a key can be accessed via:
            #   cm.ca.items.get(key) -> a list or tuple of comment tokens
            comments = cm.ca.items.get(key)

            # Recurse into nested CommentedMaps
            child_value = TPVConfig.recursively_extract_comments(value)

            # If we see any comment tokens, store them in "no_qa_codes"
            if comments and len(comments) == 4 and comments[3]:
                no_qa_codes = TPVConfig.get_noqa_codes([x.value.strip() for x in comments[3]])

                # If child_value is a dict, put comment_data under a "no_qa_codes" key
                # that your sub-models can handle. If child_value is not a dict,
                # we convert it to a dict so we can attach metadata.
                if isinstance(child_value, dict):
                    # no_qa_codes can now be parsed by the Entity model
                    child_value["no_qa_codes"] = no_qa_codes

            new_dict[key] = child_value

        return new_dict
