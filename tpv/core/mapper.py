import functools
import logging
import re

from .entities import Tool, TryNextDestinationOrFail, TryNextDestinationOrWait
from .loader import TPVConfigLoader

from galaxy.jobs.mapper import JobNotReadyException

log = logging.getLogger(__name__)


class EntityToDestinationMapper(object):

    def __init__(self, loader: TPVConfigLoader):
        self.loader = loader
        self.entities = {
            "tools": loader.tools,
            "users": loader.users,
            "roles": loader.roles
        }
        self.destinations = loader.destinations
        self.default_inherits = loader.global_settings.get('default_inherits')
        self.global_context = loader.global_settings.get('context')
        self.lookup_tool_regex = functools.lru_cache(maxsize=None)(self.__compile_tool_regex)
        self.inherit_matching_entities = functools.lru_cache(maxsize=None)(self.__inherit_matching_entities)

    def __compile_tool_regex(self, key):
        return re.compile(key)

    def _find_entities_matching_id(self, entity_list, entity_name):
        matches = []
        for key in entity_list.keys():
            if self.lookup_tool_regex(key).match(entity_name):
                matches.append(entity_list[key])
        if not matches and self.default_inherits:
            default_match = entity_list.get(self.default_inherits)
            if default_match:
                matches.append(default_match)
        return matches

    def __inherit_matching_entities(self, entity_type, entity_name):
        entity_list = self.entities.get(entity_type)
        matches = self._find_entities_matching_id(entity_list, entity_name)
        return self.inherit_entities(matches)

    def inherit_entities(self, entities):
        if entities:
            return functools.reduce(lambda a, b: b.inherit(a), entities)
        else:
            return None

    def combine_entities(self, entities):
        if entities:
            return functools.reduce(lambda a, b: b.combine(a), entities)
        else:
            return None

    def rank(self, entity, destinations, context):
        context.update({
            'entity': entity,
            'self': entity
        })
        return entity.rank_destinations(destinations, context)

    def rank_matching_destinations(self, evaluated_entities, context):
        matches = [e for e in evaluated_entities if e.matches(e, context)]
        if matches:
            return self.rank(matches[0], matches, context)
        else:
            return []

    def configure_gxy_destination(self, gxy_destination, entity):
        if entity.env:
            gxy_destination.env += [dict(name=k, value=v) for (k, v) in entity.env.items()]
        gxy_destination.params.update(entity.params or {})
        if entity.resubmit:
            gxy_destination.resubmit += entity.resubmit.values()
        return gxy_destination

    def _find_matching_entities(self, tool, user):
        tool_entity = self.inherit_matching_entities("tools", tool.id)
        if not tool_entity:
            tool_entity = Tool.from_dict(self.loader, {'id': tool.id})

        entity_list = [tool_entity]

        if user:
            role_entities = (self.inherit_matching_entities("roles", role.name)
                             for role in user.all_roles() if not role.deleted)
            # trim empty
            user_role_entities = (role for role in role_entities if role)
            user_role_entity = next(user_role_entities, None)
            if user_role_entity:
                entity_list += [user_role_entity]

            user_entity = self.inherit_matching_entities("users", user.email)
            if user_entity:
                entity_list += [user_entity]

        return entity_list

    def match_and_combine_entities(self, app, tool, user, job):
        # 1. Find the entities relevant to this job
        entity_list = self._find_matching_entities(tool, user)

        # 2. Create evaluation context - these are the common variables available within any code block
        context = {}
        context.update(self.global_context or {})
        context.update({
            'app': app,
            'tool': tool,
            'user': user,
            'job': job,
            'mapper': self
        })

        # 3. Combine entity requirements
        combined_entity = self.combine_entities(entity_list)
        context.update({
            'entity': combined_entity,
            'self': combined_entity
        })

        return context, combined_entity

    def map_to_destination(self, app, tool, user, job):
        # 1. Find, combine and evaluate entities that match this tool and user
        context, partially_combined_entity = self.match_and_combine_entities(app, tool, user, job)

        # 2. Shortlist destinations with tags that match the combined entity
        matching_dest_entities = [dest for dest in self.destinations.values()]

        # 3. Fully combine entity with matching destinations
        if matching_dest_entities:
            wait_exception_raised = False
            evaluated_entities = []
            for d in matching_dest_entities:
                try:  # An exception here signifies that a destination rule did not match
                    dest_combined_entity = d.combine(partially_combined_entity)
                    context.update({
                        'entity': dest_combined_entity,
                        'self': dest_combined_entity
                    })
                    evaluated_entity = dest_combined_entity.evaluate(context)
                    evaluated_entities.append(evaluated_entity)
                except TryNextDestinationOrFail as ef:
                    log.exception(f"Destination entity: {d} matched but could not fulfill requirements due to: {ef}."
                                  " Trying next candidate...")
                except TryNextDestinationOrWait:
                    wait_exception_raised = True
                except Exception:
                    # Anything else, fail fast
                    raise
            if wait_exception_raised:
                raise JobNotReadyException()

            if evaluated_entities:
                ranked = self.rank_matching_destinations(evaluated_entities, context)
                if ranked:
                    evaluated_destination = ranked[0]
                    gxy_destination = app.job_config.get_destination(evaluated_destination.dest_name)
                    if evaluated_destination.params.get('destination_name_override'):
                        gxy_destination.id = evaluated_destination.params.get('destination_name_override')
                    return self.configure_gxy_destination(gxy_destination, evaluated_destination)

        # No matching destinations. Throw an exception
        from galaxy.jobs.mapper import JobMappingException
        raise JobMappingException(f"No destinations are available to fulfill request: {partially_combined_entity.id}")
