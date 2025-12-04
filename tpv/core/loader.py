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
        Merge two entity maps with a clear precedence:
        1) Later configs override earlier ones.
        2) If an overriding entity declares `inherits`, graft the earlier definition
           into the top of that declared chain so shared/remote defaults flow through.

        The actual inheritance resolution happens later in process_entities; here we only
        splice the prior (e.g. remote/shared) definition into the appropriate ancestor
        so it becomes the base for the local chain.
        """

        merged: Dict[str, EntityType] = dict(entities_parent)

        def find_chain_root(entity: EntityType) -> EntityType | None:
            """
            Walk the declared inheritance chain in the new/merged config to find
            the topmost parent we know about. If a parent is missing, stop.
            """
            parent_id = entity.inherits
            visited: set[str] = set()
            root: EntityType | None = None
            while parent_id and parent_id not in visited:
                visited.add(parent_id)
                parent_entity = entities_new.get(parent_id) or merged.get(parent_id)
                if not parent_entity:
                    break
                root = parent_entity
                parent_id = parent_entity.inherits
            return root

        def graft_base_into_chain(overriding_entity: EntityType, prior_definition: EntityType) -> bool:
            """
            Attach the prior definition (e.g. remote/shared) to the top of the declared
            inheritance chain so it becomes the base ancestor. The overriding entity is
            left untouched; recompute_inheritance will later walk the chain.
            """
            root_parent = find_chain_root(overriding_entity)
            if not root_parent:
                return False
            merged[root_parent.id] = root_parent.inherit(prior_definition)
            return True

        for entity in entities_new.values():
            prior_definition = merged.get(entity.id)

            if prior_definition:
                grafted = graft_base_into_chain(entity, prior_definition)
                if grafted:
                    merged[entity.id] = entity
                else:
                    merged.pop(entity.id, None)  # keep later definitions at the end
                    merged[entity.id] = entity.inherit(prior_definition)
            else:
                merged[entity.id] = entity

        return merged

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
