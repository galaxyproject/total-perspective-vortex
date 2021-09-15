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

    def _find_resource_by_id_regex(self, resource_list, resource_name):
        default_resource = resource_list.get('default')
        # shortcut for direct match
        if resource_list.get(resource_name):
            resource = resource_list.get(resource_name)
            if default_resource:
                return resource.extend(default_resource)
            else:
                return resource
        else:
            for key in resource_list.keys():
                if re.match(key, resource_name):
                    resource = resource_list[key]
                    if default_resource:
                        return resource.extend(default_resource)
                    else:
                        return resource
            return default_resource

    def merge_evaluated(self, resources):
        merged_resource = resources[0]
        for resource in resources[1:]:
            merged_resource = merged_resource.merge(resource)
        return merged_resource

    def rank(self, resource, destinations, context):
        return resource.rank_destinations(destinations, context)

    def find_best_match(self, resource, destinations, context):
        matches = [dest for dest in destinations.values() if resource.match(dest, context)]
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

        # 2. Create evaluation context
        context = {
            'app': app,
            'tool': tool,
            'user': user,
            'job': job
        }

        # 3. Evaluate resource expressions
        evaluated_resource = [resource.evaluate(context) for resource in resource_list if resource]

        # 4. Merge resource requirements
        merged = self.merge_evaluated(evaluated_resource)

        # 5. Find best matching destination
        destination = self.find_best_match(merged, self.destinations, context)

        # 6. Return destination with params
        if destination:
            destination = app.job_config.get_destination(destination.id)
            destination.env += [dict(name=k, value=v) for (k, v) in merged.env.items()]
            destination.params.update(merged.params or {})
            return destination
        else:
            return None
