import ast
import functools
import logging
import yaml

from . import helpers
from .resources import Tool, User, Role, Destination

log = logging.getLogger(__name__)


class VortexConfigLoader(object):

    def __init__(self, destination_data: dict):
        self.compile_code_block = functools.lru_cache(maxsize=0)(self.__compile_code_block)
        validated = self.validate(destination_data)
        self.tools = validated.get('tools')
        self.users = validated.get('users')
        self.roles = validated.get('roles')
        self.destinations = validated.get('destinations')

    def __compile_code_block(self, code, as_f_string=False):
        if as_f_string:
            code_str = "f'''" + str(code) + "'''"
        else:
            code_str = str(code)
        block = ast.parse(code_str, mode='exec')
        # assumes last node is an expression
        last = ast.Expression(block.body.pop().value)
        return compile(block, '<string>', mode='exec'), compile(last, '<string>', mode='eval')

    # https://stackoverflow.com/a/39381428
    def eval_code_block(self, code, context, as_f_string=False):
        exec_block, eval_block = self.compile_code_block(code, as_f_string=as_f_string)
        locals = dict(globals())
        locals.update(context)
        locals.update({
            'helpers': helpers,
            # Don't unnecessarily compute input_size unless it's referred to
            'input_size': helpers.input_size(context['job']) if 'input_size' in str(code) else 0
        })
        exec(exec_block, locals)
        return eval(eval_block, locals)

    def validate_resources(self, resource_class: type, resource_list: dict) -> dict:
        validated = {}
        for resource_id, resource_dict in resource_list.items():
            try:
                resource_dict['id'] = resource_id
                validated[resource_id] = resource_class.from_dict(self, resource_dict)
            except Exception:
                log.exception(f"Could not load resource of type: {resource_class} with data: {resource_dict}")
        return validated

    def validate(self, destination_data: dict) -> dict:
        validated = {
            'tools': self.validate_resources(Tool, destination_data.get('tools', {})),
            'users': self.validate_resources(User, destination_data.get('users', {})),
            'roles': self.validate_resources(Role, destination_data.get('roles', {})),
            'destinations': self.validate_resources(Destination, destination_data.get('destinations', {}))
        }
        return validated

    @staticmethod
    def from_file_path(path: str):
        with open(path, 'r') as f:
            dest_data = yaml.safe_load(f)
            return VortexConfigLoader(dest_data)
