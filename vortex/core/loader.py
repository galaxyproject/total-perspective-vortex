from __future__ import annotations
import ast
import functools
import logging
import os
import yaml

import requests

from . import helpers
from .entities import Tool, User, Role, Destination, Entity

log = logging.getLogger(__name__)


class InvalidParentException(Exception):
    pass


class VortexConfigLoader(object):

    def __init__(self, vortex_config: dict):
        self.compile_code_block = functools.lru_cache(maxsize=None)(self.__compile_code_block)
        self.global_settings = vortex_config.get('global', {})
        entities = self.load_entities(vortex_config)
        self.tools = entities.get('tools')
        self.users = entities.get('users')
        self.roles = entities.get('roles')
        self.destinations = entities.get('destinations')

    def __compile_code_block(self, code, as_f_string=False, exec_only=False):
        if as_f_string:
            code_str = "f'''" + str(code) + "'''"
        else:
            code_str = str(code)
        block = ast.parse(code_str, mode='exec')
        if exec_only:
            return compile(block, '<string>', mode='exec'), None
        else:
            # assumes last node is an expression
            last = ast.Expression(block.body.pop().value)
            return compile(block, '<string>', mode='exec'), compile(last, '<string>', mode='eval')

    # https://stackoverflow.com/a/39381428
    def eval_code_block(self, code, context, as_f_string=False, exec_only=False):
        exec_block, eval_block = self.compile_code_block(code, as_f_string=as_f_string, exec_only=exec_only)
        locals = dict(globals())
        locals.update(context)
        locals.update({
            'helpers': helpers,
            # Don't unnecessarily compute input_size unless it's referred to
            'input_size': helpers.input_size(context['job']) if 'input_size' in str(code) else 0
        })
        exec(exec_block, locals)
        if eval_block:
            return eval(eval_block, locals)
        else:
            return None

    def process_inheritance(self, entity_list: dict[str, Entity], entity: Entity):
        if entity.inherits:
            parent_entity = entity_list.get(entity.inherits)
            if not parent_entity:
                raise InvalidParentException(f"The specified parent: {entity.inherits} for"
                                             f" entity: {entity} does not exist")
            return entity.inherit(self.process_inheritance(entity_list, parent_entity))
        else:
            default_inherits = self.global_settings.get('default_inherits')
            if default_inherits and not entity.id == default_inherits:
                default_parent = entity_list.get(default_inherits)
                return entity.inherit(default_parent)
            else:
                return entity

    def recompute_inheritance(self, entities: dict[str, Entity]):
        for key, entity in entities.items():
            entities[key] = self.process_inheritance(entities, entity)

    def validate_entities(self, entity_class: type, entity_list: dict) -> dict:
        validated = {}
        for entity_id, entity_dict in entity_list.items():
            try:
                if not entity_dict:
                    entity_dict = {}
                entity_dict['id'] = entity_id
                entity_class.from_dict(self, entity_dict)
                validated[entity_id] = entity_class.from_dict(self, entity_dict)
            except Exception:
                log.exception(f"Could not load entity of type: {entity_class} with data: {entity_dict}")
                raise
        self.recompute_inheritance(validated)
        return validated

    def load_entities(self, vortex_config: dict) -> dict:
        validated = {
            'tools': self.validate_entities(Tool, vortex_config.get('tools', {})),
            'users': self.validate_entities(User, vortex_config.get('users', {})),
            'roles': self.validate_entities(Role, vortex_config.get('roles', {})),
            'destinations': self.validate_entities(Destination, vortex_config.get('destinations', {}))
        }
        return validated

    def inherit_existing_entities(self, entities_current, entities_new):
        for entity in entities_new.values():
            if entities_current.get(entity.id):
                entities_current[entity.id] = entity.inherit(entities_current.get(entity.id))
            else:
                entities_current[entity.id] = entity
        self.recompute_inheritance(entities_current)

    def merge_loader(self, loader: VortexConfigLoader):
        self.global_settings.update(loader.global_settings)
        self.inherit_existing_entities(self.tools, loader.tools)
        self.inherit_existing_entities(self.users, loader.users)
        self.inherit_existing_entities(self.roles, loader.roles)
        self.inherit_existing_entities(self.destinations, loader.destinations)

    @staticmethod
    def from_url_or_path(url_or_path: str):
        if os.path.isfile(url_or_path):
            with open(url_or_path, 'r') as f:
                vortex_config = yaml.safe_load(f)
                return VortexConfigLoader(vortex_config)
        else:
            with requests.get(url_or_path) as r:
                vortex_config = yaml.safe_load(r.content)
                return VortexConfigLoader(vortex_config)
