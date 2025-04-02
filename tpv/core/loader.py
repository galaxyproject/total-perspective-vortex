from __future__ import annotations

import ast
import functools
import logging
from typing import Dict

from . import helpers, util
from .entities import Entity, GlobalConfig, TPVConfig
from .evaluator import TPVCodeEvaluator

log = logging.getLogger(__name__)


class InvalidParentException(Exception):
    pass


class TPVConfigLoader(TPVCodeEvaluator):

    def __init__(
        self, tpv_config: dict | TPVConfig, parent: TPVConfigLoader | None = None
    ):
        self.compile_code_block = functools.lru_cache(maxsize=None)(
            self.__compile_code_block
        )
        tpv_config["evaluator"] = self
        self.config = TPVConfig.model_validate(tpv_config)
        if parent:
            self.merge_config(parent.config)
        else:
            self.process_entities(self.config)

    def compile_code_block(self, code, as_f_string=False, exec_only=False):
        # interface method, replaced with instance based lru cache in constructor
        pass

    def __compile_code_block(self, code, as_f_string=False, exec_only=False):
        if as_f_string:
            code_str = "f'''" + str(code) + "'''"
        else:
            code_str = str(code)
        block = ast.parse(code_str, mode="exec")
        if exec_only:
            return compile(block, "<string>", mode="exec"), None
        else:
            # assumes last node is an expression
            last = ast.Expression(block.body.pop().value)
            return compile(block, "<string>", mode="exec"), compile(
                last, "<string>", mode="eval"
            )

    # https://stackoverflow.com/a/39381428
    def eval_code_block(self, code, context, as_f_string=False, exec_only=False):
        exec_block, eval_block = self.compile_code_block(
            code, as_f_string=as_f_string, exec_only=exec_only
        )
        locals = dict(globals())
        locals.update(context)
        locals.update(
            {
                "helpers": helpers,
                # Don't unnecessarily compute input_size unless it's referred to
                "input_size": (
                    helpers.input_size(context["job"])
                    if "input_size" in str(code)
                    else 0
                ),
            }
        )
        exec(exec_block, locals)
        if eval_block:
            return eval(eval_block, locals)
        else:
            return None

    @staticmethod
    def process_inheritance(entity_list: dict[str, Entity], entity: Entity):
        if entity.inherits:
            parent_entity = entity_list.get(entity.inherits)
            if not parent_entity:
                raise InvalidParentException(
                    f"The specified parent: {entity.inherits} for"
                    f" entity: {entity} does not exist"
                )
            return entity.inherit(
                TPVConfigLoader.process_inheritance(entity_list, parent_entity)
            )
        # do not process default inheritance here, only at runtime, as multiple can cause default inheritance
        # to override later matches.
        return entity

    @staticmethod
    def recompute_inheritance(entities: dict[str, Entity]):
        for key, entity in entities.items():
            entities[key] = TPVConfigLoader.process_inheritance(entities, entity)
        return entities

    def validate_entities(self, entities: Dict[str, Entity]) -> dict:
        self.recompute_inheritance(entities)

    def process_entities(self, tpv_config: TPVConfig) -> dict:
        self.validate_entities(tpv_config.tools),
        self.validate_entities(tpv_config.users),
        self.validate_entities(tpv_config.roles),
        self.validate_entities(tpv_config.destinations)

    def inherit_globals(self, parent_globals: GlobalConfig):
        if parent_globals:
            self.config.global_config.default_inherits = (
                self.config.global_config.default_inherits
                or parent_globals.default_inherits
            )
            merged_context = dict(parent_globals.context or {})
            merged_context.update(self.config.global_config.context)
            self.config.global_config.context = merged_context

    def inherit_parent_entities(
        self, entities_parent: dict[str, Entity], entities_new: dict[str, Entity]
    ):
        for entity in entities_new.values():
            if entities_parent.get(entity.id):
                parent_entity = entities_parent.get(entity.id)
                del entities_parent[entity.id]
                # reinsert at the end
                entities_parent[entity.id] = entity.inherit(parent_entity)
            else:
                entities_parent[entity.id] = entity
        return self.recompute_inheritance(entities_parent)

    def merge_config(self, parent_config: TPVConfig):
        self.inherit_globals(parent_config.global_config)
        self.config.tools = self.inherit_parent_entities(
            parent_config.tools, self.config.tools
        )
        self.config.users = self.inherit_parent_entities(
            parent_config.users, self.config.users
        )
        self.config.roles = self.inherit_parent_entities(
            parent_config.roles, self.config.roles
        )
        self.config.destinations = self.inherit_parent_entities(
            parent_config.destinations, self.config.destinations
        )

    @staticmethod
    def from_url_or_path(url_or_path: str, parent: TPVConfigLoader | None = None):
        tpv_config = util.load_yaml_from_url_or_path(url_or_path)
        try:
            return TPVConfigLoader(tpv_config, parent=parent)
        except Exception as e:
            log.exception(f"Error loading TPV config: {url_or_path}")
            raise e
