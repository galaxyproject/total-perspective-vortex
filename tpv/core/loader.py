from __future__ import annotations
import ast
import functools
import logging
import re

from . import helpers
from . import util
from .entities import Tool, User, Role, Destination, Entity

log = logging.getLogger(__name__)


NOQA_RE = re.compile(r"#\s*noqa:\s*([A-Z0-9, ]+)?")


class InvalidParentException(Exception):
    pass


class TPVConfigLoader(object):

    def __init__(self, tpv_config: dict):
        self.compile_code_block = functools.lru_cache(maxsize=None)(self.__compile_code_block)
        self.global_settings = tpv_config.get('global', {})
        self.noqa = {'tools': {}, 'users': {}, 'roles': {}, 'destinations': {}}
        entities = self.load_entities(tpv_config)
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
        # do not process default inheritance here, only at runtime, as multiple can cause default inheritance
        # to override later matches.
        return entity

    def recompute_inheritance(self, entities: dict[str, Entity]):
        for key, entity in entities.items():
            entities[key] = self.process_inheritance(entities, entity)

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

    def validate_entities(self, entity_class: type, entity_list: dict, noqa_dict: dict) -> dict:
        # This code relies on dict ordering guarantees provided since python 3.6
        validated = {}
        for entity_id, entity_dict in entity_list.items():
            try:
                if not entity_dict:
                    entity_dict = {}
                entity_dict['id'] = entity_id
                validated[entity_id] = entity_class.from_dict(self, entity_dict)
            except Exception:
                log.exception(f"Could not load entity of type: {entity_class} with data: {entity_dict}")
                raise
            self.store_noqa_codes(entity_list, entity_id, noqa_dict)
        self.recompute_inheritance(validated)
        return validated

    def load_entities(self, tpv_config: dict) -> dict:
        validated = {
            'tools': self.validate_entities(Tool, tpv_config.get('tools', {}), self.noqa['tools']),
            'users': self.validate_entities(User, tpv_config.get('users', {}), self.noqa['users']),
            'roles': self.validate_entities(Role, tpv_config.get('roles', {}), self.noqa['roles']),
            'destinations': self.validate_entities(
                Destination,
                tpv_config.get('destinations', {}),
                self.noqa['destinations'])
        }
        return validated

    def inherit_globals(self, globals_other):
        if globals_other:
            self.global_settings.update({'default_inherits': globals_other.get('default_inherits')}
                                        if globals_other.get('default_inherits') else {})
            self.global_settings['context'] = self.global_settings.get('context') or {}
            self.global_settings['context'].update(globals_other.get('context') or {})

    def inherit_existing_entities(self, entities_current, entities_new):
        for entity in entities_new.values():
            if entities_current.get(entity.id):
                current_entity = entities_current.get(entity.id)
                del entities_current[entity.id]
                # reinsert at the end
                entities_current[entity.id] = entity.inherit(current_entity)
            else:
                entities_current[entity.id] = entity
        self.recompute_inheritance(entities_current)

    def merge_loader(self, loader: TPVConfigLoader):
        self.inherit_globals(loader.global_settings)
        self.inherit_existing_entities(self.tools, loader.tools)
        self.inherit_existing_entities(self.users, loader.users)
        self.inherit_existing_entities(self.roles, loader.roles)
        self.inherit_existing_entities(self.destinations, loader.destinations)

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
        return TPVConfigLoader(tpv_config)
