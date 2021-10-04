import functools
import logging
import re
from typing import Dict

from .resources import Tool, User, Role, Destination

log = logging.getLogger(__name__)


class ResourceToDestinationMapper(object):

    def __init__(self, tools: Dict[str, Tool], users: Dict[str, User], roles: Dict[str, Role],
                 destinations: Dict[str, Destination]):
        self.tools = tools
        self.users = users
        self.roles = roles
        self.destinations = destinations
        self.lookup_tool_regex = functools.lru_cache(maxsize=0)(self.__compile_tool_regex)

    def __compile_tool_regex(self, key):
        return re.compile(key)

    def _find_resource_by_id_regex(self, resource_list, resource_name):
        # shortcut for direct match
        if resource_list.get(resource_name):
            return resource_list.get(resource_name)
        else:
            for key in resource_list.keys():
                if self.lookup_tool_regex(key).match(resource_name):
                    return resource_list[key]
            return resource_list.get('default')

    def merge_resources(self, resources):
        merged_resource = resources[0]
        for resource in resources[1:]:
            merged_resource = merged_resource.merge(resource)
        return merged_resource

    def rank(self, resource, destinations, context):
        return resource.rank_destinations(destinations, context)

    def find_best_match(self, resource, destinations, context):
        matches = [dest for dest in destinations.values() if resource.matches(dest, context)]
        rankings = self.rank(resource, matches, context)
        return rankings[0] if rankings else None

    def _find_matching_resources(self, tool, user):
        tool_resource = self._find_resource_by_id_regex(self.tools, tool.id)

        resource_list = [tool_resource]

        if user:
            user_resource = self._find_resource_by_id_regex(self.users, user.email)
            if user_resource:
                resource_list += [user_resource]

            role_resources = (self._find_resource_by_id_regex(self.roles, role.name)
                              for role in user.all_roles() if not role.deleted)
            # trim empty
            user_role_resources = (role for role in role_resources if role)
            user_role_resource = next(user_role_resources, None)
            if user_role_resource:
                resource_list += [user_role_resource]

        return resource_list

    def map_to_destination(self, app, tool, user, job):
        # 1. Find the resources relevant to this job
        resource_list = self._find_matching_resources(tool, user)

        # 2. Merge resource requirements
        merged_resource = self.merge_resources(resource_list)

        # 3. Create evaluation context - these are the common variables available within any code block
        context = {
            'app': app,
            'tool': tool,
            'user': user,
            'job': job,
            'mapper': self,
            'resource': merged_resource,
            'self': merged_resource
        }

        # 4. Evaluate resource expressions
        evaluated_resource = merged_resource.evaluate(context)

        # 5. Find best matching destination
        destination = self.find_best_match(evaluated_resource, self.destinations, context)

        # 6. Return destination with params
        if destination:
            destination = app.job_config.get_destination(destination.id)
            destination.env += [dict(name=k, value=v) for (k, v) in evaluated_resource.env.items()]
            destination.params.update(evaluated_resource.params or {})
            return destination
        else:
            return None
