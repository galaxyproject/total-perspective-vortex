from __future__ import annotations

import ast
import functools
import logging
from collections.abc import Callable
from types import CodeType
from typing import Any, Dict, TypeVar, cast

from . import helpers, util
from .entities import Entity, GlobalConfig, TPVConfig
from .evaluator import TPVCodeEvaluator

log = logging.getLogger(__name__)


EntityType = TypeVar("EntityType", bound=Entity)


class InvalidParentException(Exception):
    pass


class TPVConfigLoader(TPVCodeEvaluator):

    def __init__(self, tpv_config: Dict[Any, Any], parent: TPVConfigLoader | None = None):
        self._cached_compile_code_block: Callable[[str, bool, bool], tuple[CodeType, CodeType | None]] = (
            functools.lru_cache(maxsize=None)(self.__compile_code_block)
        )
        tpv_config["evaluator"] = self
        self.config = TPVConfig.model_validate(tpv_config)
        if parent:
            self.merge_config(parent.config)
        self.process_entities(self.config)

    def compile_code_block(
        self, code: str, as_f_string: bool = False, exec_only: bool = False
    ) -> tuple[CodeType, CodeType | None]:
        return self._cached_compile_code_block(code, as_f_string, exec_only)

    def __compile_code_block(
        self, code: str, as_f_string: bool = False, exec_only: bool = False
    ) -> tuple[CodeType, CodeType | None]:
        if as_f_string:
            code_str = "f'''" + str(code) + "'''"
        else:
            code_str = str(code)
        block = ast.parse(code_str, mode="exec")
        if exec_only:
            return compile(block, "<string>", mode="exec"), None
        else:
            # assumes last node is an expression
            last_stmt = block.body.pop()
            assert isinstance(last_stmt, ast.Expr)
            last = ast.Expression(last_stmt.value)
            return compile(block, "<string>", mode="exec"), compile(last, "<string>", mode="eval")

    # https://stackoverflow.com/a/39381428
    def eval_code_block(
        self,
        code: str,
        context: Dict[str, Any],
        as_f_string: bool = False,
        exec_only: bool = False,
    ) -> Any:
        exec_block, eval_block = self.compile_code_block(code, as_f_string=as_f_string, exec_only=exec_only)
        locals = dict(globals())
        locals.update(context)
        locals.update(
            {
                "helpers": helpers,
                # Don't unnecessarily compute input_size unless it's referred to
                "input_size": (helpers.input_size(context["job"]) if "input_size" in str(code) else 0),
            }
        )
        exec(exec_block, locals)
        if eval_block:
            return eval(eval_block, locals)
        else:
            return None

    @staticmethod
    def process_inheritance(entity_list: Dict[str, EntityType], entity: EntityType) -> EntityType:
        if entity.inherits:
            parent_entity = entity_list.get(entity.inherits)
            if not parent_entity:
                raise InvalidParentException(
                    f"The specified parent: {entity.inherits} for" f" entity: {entity} does not exist"
                )
            return entity.inherit(TPVConfigLoader.process_inheritance(entity_list, parent_entity))
        # do not process default inheritance here, only at runtime, as multiple can cause default inheritance
        # to override later matches.
        return entity

    @staticmethod
    def recompute_inheritance(entities: Dict[str, EntityType]) -> None:
        for key, entity in entities.items():
            entities[key] = TPVConfigLoader.process_inheritance(entities, entity)

    def process_entities(self, tpv_config: TPVConfig) -> None:
        self.recompute_inheritance(tpv_config.tools)
        self.recompute_inheritance(tpv_config.users)
        self.recompute_inheritance(tpv_config.roles)
        self.recompute_inheritance(tpv_config.destinations)

    def inherit_globals(self, parent_globals: GlobalConfig) -> None:
        if parent_globals:
            self.config.global_config.default_inherits = (
                self.config.global_config.default_inherits or parent_globals.default_inherits
            )
            merged_context = dict(parent_globals.context or {})
            merged_context.update(self.config.global_config.context)
            self.config.global_config.context = merged_context

    def inherit_parent_entities(
        self,
        entities_parent: Dict[str, EntityType],
        entities_new: Dict[str, EntityType],
    ) -> Dict[str, EntityType]:
        """
        Merge parent and new entity mappings. Later definitions override earlier ones.
        If a new entity declares `inherits`, graft the existing definition as the base
        of that chain so shared/remote defaults flow through while local parents can
        still override.
        """
        merged_entities = dict(entities_parent)

        for entity in entities_new.values():
            parent_entity = merged_entities.get(entity.id)

            if parent_entity is not None and entity.inherits:
                root_parent_id = entity.inherits
                root_parent_entity = entities_new.get(root_parent_id) or merged_entities.get(root_parent_id)
                if root_parent_entity:
                    if root_parent_id in merged_entities:
                        merged_entities.pop(root_parent_id)
                    merged_entities[root_parent_id] = root_parent_entity.inherit(parent_entity)
                    if entity.id in merged_entities:
                        merged_entities.pop(entity.id)
                    merged_entities[entity.id] = entity
                    continue
                else:
                    entity = entity.inherit(parent_entity)

            if parent_entity is not None:
                merged_entities.pop(entity.id, None)
                merged_entities[entity.id] = entity.inherit(parent_entity)
            else:
                merged_entities[entity.id] = entity

        return merged_entities

    def merge_config(self, parent_config: TPVConfig) -> None:
        self.inherit_globals(parent_config.global_config)
        self.config.tools = self.inherit_parent_entities(parent_config.tools, self.config.tools)
        self.config.users = self.inherit_parent_entities(parent_config.users, self.config.users)
        self.config.roles = self.inherit_parent_entities(parent_config.roles, self.config.roles)
        self.config.destinations = self.inherit_parent_entities(parent_config.destinations, self.config.destinations)

    @staticmethod
    def from_url_or_path(url_or_path: str, parent: TPVConfigLoader | None = None) -> TPVConfigLoader:
        tpv_config = util.load_yaml_from_url_or_path(url_or_path)
        try:
            return TPVConfigLoader(tpv_config, parent=parent)
        except Exception as e:
            log.exception(f"Error loading TPV config: {url_or_path}")
            raise e
