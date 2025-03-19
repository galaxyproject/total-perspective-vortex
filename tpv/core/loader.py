from __future__ import annotations

import ast
import functools
import logging
from typing import Dict
import re

from . import helpers, util
from .entities import Entity, GlobalConfig, TPVConfig
from .evaluator import TPVCodeEvaluator

log = logging.getLogger(__name__)


NOQA_RE = re.compile(r"#\s*noqa:\s*([A-Z0-9, ]+)?")


class InvalidParentException(Exception):
    pass


class TPVConfigLoader(TPVCodeEvaluator):

    def __init__(self, tpv_config: TPVConfig):
        self.compile_code_block = functools.lru_cache(maxsize=None)(
            self.__compile_code_block
        )
        self.config = TPVConfig(evaluator=self, **tpv_config)
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

    def process_inheritance(self, entity_list: dict[str, Entity], entity: Entity):
        if entity.inherits:
            parent_entity = entity_list.get(entity.inherits)
            if not parent_entity:
                raise InvalidParentException(
                    f"The specified parent: {entity.inherits} for"
                    f" entity: {entity} does not exist"
                )
            return entity.inherit(self.process_inheritance(entity_list, parent_entity))
        # do not process default inheritance here, only at runtime, as multiple can cause default inheritance
        # to override later matches.
        return entity

    def recompute_inheritance(self, entities: dict[str, Entity]):
        for key, entity in entities.items():
            entities[key] = self.process_inheritance(entities, entity)

    def validate_entities(self, entities: Dict[str, Entity]) -> dict:
        self.recompute_inheritance(entities)

    def get_noqa_codes(self, entity_comments: list) -> (bool, set[str] | None):
        comments = []
        if entity_comments and len(entity_comments) == 4 and entity_comments[3]:
            comments.extend([x.value.strip() for x in entity_comments[3]])

        for comment in comments:
            match = re.match(r"#\s*noqa:?\s*([A-Z0-9, ]+)?", comment)
            if match:
                codes = match.group(1)
                # Return a set of codes or None if `# noqa` with no codes
                return (True, set(code.strip() for code in codes.split(',')) if codes else None)

        return (False, None)

    def store_noqa_codes(self, entity_list: dict, entity_id: str, noqa_dict: dict):
        if hasattr(entity_list, "ca"):
            entity_comments = entity_list.ca.items.get(entity_id)
            noqa, noqa_codes = self.get_noqa_codes(entity_comments)
            if noqa:
                noqa_dict[entity_id] = noqa_codes

    def process_entities(self, tpv_config: TPVConfig) -> dict:
        self.validate_entities(tpv_config.tools),
        self.validate_entities(tpv_config.users),
        self.validate_entities(tpv_config.roles),
        self.validate_entities(tpv_config.destinations)

    def inherit_globals(self, globals_other: GlobalConfig):
        if globals_other:
            self.config.global_config.default_inherits = (
                globals_other.default_inherits
                or self.config.global_config.default_inherits
            )
            self.config.global_config.context.update(globals_other.context)

    def inherit_existing_entities(
        self, entities_current: dict[str, Entity], entities_new: dict[str, Entity]
    ):
        for entity in entities_new.values():
            if entities_current.get(entity.id):
                current_entity = entities_current.get(entity.id)
                del entities_current[entity.id]
                # reinsert at the end
                entities_current[entity.id] = entity.inherit(current_entity)
            else:
                entities_current[entity.id] = entity
        self.recompute_inheritance(entities_current)

    def merge_config(self, config: TPVConfig):
        self.inherit_globals(config.global_config)
        self.inherit_existing_entities(self.config.tools, config.tools)
        self.inherit_existing_entities(self.config.users, config.users)
        self.inherit_existing_entities(self.config.roles, config.roles)
        self.inherit_existing_entities(self.config.destinations, config.destinations)

    def merge_loader(self, loader: TPVConfigLoader):
        self.merge_config(loader.config)

    def check_noqa(self, entity: Entity, code: str) -> bool:
        if type(entity) is Tool:
            noqa = self.noqa['tools']
        elif type(entity) is User:
            noqa = self.noqa['users']
        elif type(entity) is Role:
            noqa = self.noqa['roles']
        elif type(entity) is Destination:
            noqa = self.noqa['destinations']
        else:
            raise RuntimeError(f"Unknown entity type: {entity}")
        if entity.id in noqa and (noqa[entity.id] is None or code in noqa[entity.id]):
            return True
        return False

    @staticmethod
    def from_url_or_path(url_or_path: str):
        tpv_config = util.load_yaml_from_url_or_path(url_or_path)
        try:
            return TPVConfigLoader(tpv_config)
        except Exception as e:
            log.exception(f"Error loading TPV config: {url_or_path}")
            raise e
