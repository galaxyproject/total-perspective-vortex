import re
from typing import Dict

from .resources import Tool, User, Group, ResourceDestinationParser


class ResourceToDestinationMapper(object):

    def __init__(self, tools: Dict[str, Tool], users: Dict[str, User], groups: Dict[str, Group], destinations):
        self.tools = tools
        self.users = users
        self.groups = groups
        self.destinations = destinations

    def _find_resource_by_id_regex(self, resource_list, resource_name):
        # shortcut for direct match
        if resource_list.get(resource_name):
            return resource_list.get(resource_name)
        else:
            for key in resource_list.keys():
                if re.match(key, resource_name):
                    return resource_list[key]
            return resource_list.get('default')

    def merge_resources(self, resources):
        merged_resource = resources[0]
        for resource in resources[1:]:
            if resource:
                merged_resource = merged_resource.merge(resource)
        return merged_resource

    def rank(self, resource, destinations):
        return destinations

    def find_best_match(self, resource, destinations):
        matches = (dest[0] for dest in destinations.values() if resource.matches_destination(dest[0]))
        return next(self.rank(resource, matches), None)

    def map_to_destination(self, tool, user, job):
        tool_resource = self._find_resource_by_id_regex(self.tools, tool.id)
        user_resource = self._find_resource_by_id_regex(self.users, user.email)
        role_resources = [self._find_resource_by_id_regex(self.groups, role)
                          for role in user.all_roles() if not role.deleted]
        # trim empty
        user_role_resources = [role for role in role_resources if role]

        resource_list = [tool_resource, user_resource]
        if user_role_resources:
            resource_list += [user_role_resources[0]]
        merged = self.merge_resources(resource_list)

        return self.find_best_match(merged, self.destinations)


ACTIVE_DESTINATION_MAPPER = None


def map_tool_to_destination(app, job, tool, user, mapper_config_file):
    global ACTIVE_DESTINATION_MAPPER
    if not ACTIVE_DESTINATION_MAPPER:
        parser = ResourceDestinationParser.from_file_path(mapper_config_file)
        ACTIVE_DESTINATION_MAPPER = ResourceToDestinationMapper(parser.tools, parser.users, parser.groups,
                                                                app.job_config.destinations)
    return ACTIVE_DESTINATION_MAPPER.map_to_destination(tool, user, job)
