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


class InvalidParentException(Exception):
    pass


class VortexConfigLoader(object):

    def __init__(self, vortex_config: dict):
        self.compile_code_block = functools.lru_cache(maxsize=None)(self.__compile_code_block)
        self.global_settings = vortex_config.get('global', {})
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

    def process_inheritance(self, resource_list: dict[str, Resource], resource: Resource):
        if resource.inherits:
            parent_resource = resource_list.get(resource.inherits)
            if not parent_resource:
                raise InvalidParentException(f"The specified parent: {resource.inherits} for"
                                             f" resource: {resource} does not exist")
            return resource.inherit(self.process_inheritance(resource_list, parent_resource))
        else:
            default_inherits = self.global_settings.get('default_inherits')
            if default_inherits and not resource.id == default_inherits:
                default_parent = resource_list.get(default_inherits)
                return resource.inherit(default_parent)
            else:
                return resource

    def recompute_inheritance(self, resources: dict[str, Resource]):
        for key, resource in resources.items():
            resources[key] = self.process_inheritance(resources, resource)

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
        self.recompute_inheritance(validated)
        return validated

    def load_resources(self, vortex_config: dict) -> dict:
        validated = {
            'tools': self.validate_resources(Tool, vortex_config.get('tools', {})),
            'users': self.validate_resources(User, vortex_config.get('users', {})),
            'roles': self.validate_resources(Role, vortex_config.get('roles', {})),
            'destinations': self.validate_resources(Destination, vortex_config.get('destinations', {}))
        }
        return validated

    def inherit_existing_resources(self, resources_current, resources_new):
        for resource in resources_new.values():
            if resources_current.get(resource.id):
                resources_current[resource.id] = resource.inherit(resources_current.get(resource.id))
            else:
                resources_current[resource.id] = resource
        self.recompute_inheritance(resources_current)

    def merge_loader(self, loader: VortexConfigLoader):
        self.global_settings.update(loader.global_settings)
        self.inherit_existing_resources(self.tools, loader.tools)
        self.inherit_existing_resources(self.users, loader.users)
        self.inherit_existing_resources(self.roles, loader.roles)
        self.inherit_existing_resources(self.destinations, loader.destinations)

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
