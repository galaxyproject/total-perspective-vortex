import logging
import yaml

from .resources import Tool, User, Role, Destination

log = logging.getLogger(__name__)


class VortexConfigLoader(object):

    def __init__(self, destination_data: dict):
        validated = self.validate(destination_data)
        self.tools = validated.get('tools')
        self.users = validated.get('users')
        self.roles = validated.get('roles')
        self.destinations = validated.get('destinations')

    @staticmethod
    def validate_resources(resource_class: type, resource_list: dict) -> dict:
        validated = {}
        for resource_id, resource_dict in resource_list.items():
            try:
                resource_dict['id'] = resource_id
                validated[resource_id] = resource_class.from_dict(resource_dict)
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
