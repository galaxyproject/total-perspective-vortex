from __future__ import annotations
import ast
import functools
import logging
import os
import yaml

import requests

from . import helpers
from .resources import Tool, User, Role, Destination, Resource

log = logging.getLogger(__name__)


class VortexConfigLoader(object):

    def __init__(self, vortex_config: dict):
        self.compile_code_block = functools.lru_cache(maxsize=None)(self.__compile_code_block)
        resources = self.load_resources(vortex_config)
        self.tools = resources.get('tools')
        self.users = resources.get('users')
        self.roles = resources.get('roles')
        self.destinations = resources.get('destinations')

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

    @staticmethod
    def process_default_inheritance(resources: dict[str, Resource]):
        default_resource = resources.get('default')
        for key in resources.keys():
            if default_resource and not key == "default":
                resources[key] = resources[key].extend(default_resource)

    def validate_resources(self, resource_class: type, resource_list: dict) -> dict:
        validated = {}
        for resource_id, resource_dict in resource_list.items():
            try:
                resource_dict['id'] = resource_id
                resource_class.from_dict(self, resource_dict)
                validated[resource_id] = resource_class.from_dict(self, resource_dict)
            except Exception:
                log.exception(f"Could not load resource of type: {resource_class} with data: {resource_dict}")
                raise
        self.process_default_inheritance(validated)
        return validated

    def load_resources(self, vortex_config: dict) -> dict:
        validated = {
            'tools': self.validate_resources(Tool, vortex_config.get('tools', {})),
            'users': self.validate_resources(User, vortex_config.get('users', {})),
            'roles': self.validate_resources(Role, vortex_config.get('roles', {})),
            'destinations': self.validate_resources(Destination, vortex_config.get('destinations', {}))
        }
        return validated

    def extend_existing_resources(self, resources_current, resources_new):
        for resource in resources_new.values():
            if resources_current.get(resource.id):
                resources_current[resource.id] = resource.extend(resources_current.get(resource.id))
            else:
                resources_current[resource.id] = resource
        self.process_default_inheritance(resources_current)

    def merge_loader(self, loader: VortexConfigLoader):
        self.extend_existing_resources(self.tools, loader.tools)
        self.extend_existing_resources(self.users, loader.users)
        self.extend_existing_resources(self.roles, loader.roles)
        self.extend_existing_resources(self.destinations, loader.destinations)

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
