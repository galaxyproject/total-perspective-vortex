import logging
import re
from typing import Dict

from galaxy.util.watcher import get_watcher

from .resources import Tool, User, Role, Destination, ResourceDestinationParser

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

    def rank(self, resource, destinations):
        return destinations

    def find_best_match(self, resource, destinations, context):
        matches = (dest for dest in destinations.values() if resource.matches_destination(dest, context))
        return next(self.rank(resource, matches), None)

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

        # 2. Create context
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


ACTIVE_DESTINATION_MAPPER = None


def load_destination_mapper(mapper_config_file):
    log.info(f"loading vortex rules from: {mapper_config_file}")
    parser = ResourceDestinationParser.from_file_path(mapper_config_file)
    return ResourceToDestinationMapper(parser.tools, parser.users, parser.roles, parser.destinations)


def reload_destination_mapper(mapper_config_file):
    log.info(f"reloading vortex rules from: {mapper_config_file}")
    global ACTIVE_DESTINATION_MAPPER
    ACTIVE_DESTINATION_MAPPER = load_destination_mapper(mapper_config_file)


def setup_destination_mapper(app, mapper_config_file):
    mapper = load_destination_mapper(mapper_config_file)
    job_rule_watcher = get_watcher(app.config, 'watch_job_rules', monitor_what_str='job rules')
    job_rule_watcher.watch_file(mapper_config_file, callback=reload_destination_mapper)
    job_rule_watcher.start()
    return mapper


def map_tool_to_destination(app, job, tool, user, mapper_config_file):
    global ACTIVE_DESTINATION_MAPPER
    if not ACTIVE_DESTINATION_MAPPER:
        ACTIVE_DESTINATION_MAPPER = setup_destination_mapper(app, mapper_config_file)
    return ACTIVE_DESTINATION_MAPPER.map_to_destination(app, tool, user, job)
