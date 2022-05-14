import functools
import logging
import re

from .entities import Tool
from .loader import VortexConfigLoader

log = logging.getLogger(__name__)


class EntityToDestinationMapper(object):

    def __init__(self, loader: VortexConfigLoader):
        self.loader = loader
        self.tools = loader.tools
        self.users = loader.users
        self.roles = loader.roles
        self.destinations = loader.destinations
        self.default_inherits = loader.global_settings.get('default_inherits')
        self.lookup_tool_regex = functools.lru_cache(maxsize=None)(self.__compile_tool_regex)

    def __compile_tool_regex(self, key):
        return re.compile(key)

    def _find_entity_by_id_regex(self, entity_list, entity_name):
        # shortcut for direct match
        if entity_list.get(entity_name):
            return entity_list.get(entity_name)
        else:
            for key in entity_list.keys():
                if self.lookup_tool_regex(key).match(entity_name):
                    return entity_list[key]
            if self.default_inherits:
                return entity_list.get(self.default_inherits)
            return None

    def evaluate_early(self, entities, context):
        evaluated = []
        for entity in entities:
            context.update({
                'entities': entities,
                'entity': entity,
                'self': entity
            })
            evaluated.append(entity.evaluate_early(context))
        return evaluated

    def combine_entities(self, entities):
        combined_entity = entities[0]
        for entity in entities[1:]:
            combined_entity = combined_entity.combine(entity)
        return combined_entity

    def rank(self, entity, destinations, context):
        return entity.rank_destinations(destinations, context)

    def find_best_match(self, entity, destinations, context):
        matches = [dest for dest in destinations.values() if entity.matches(dest, context)]
        rankings = self.rank(entity, matches, context)
        return rankings[0] if rankings else None

    def _find_matching_entities(self, tool, user):
        tool_entity = self._find_entity_by_id_regex(self.tools, tool.id)
        if not tool_entity:
            tool_entity = Tool.from_dict(self.loader, {'id': tool.id})

        entity_list = [tool_entity]

        if user:
            role_entities = (self._find_entity_by_id_regex(self.roles, role.name)
                             for role in user.all_roles() if not role.deleted)
            # trim empty
            user_role_entities = (role for role in role_entities if role)
            user_role_entity = next(user_role_entities, None)
            if user_role_entity:
                entity_list += [user_role_entity]

            user_entity = self._find_entity_by_id_regex(self.users, user.email)
            if user_entity:
                entity_list += [user_entity]

        return entity_list

    def map_to_destination(self, app, tool, user, job):
        # 1. Find the entities relevant to this job
        entity_list = self._find_matching_entities(tool, user)

        # 2. Create evaluation context - these are the common variables available within any code block
        context = {
            'app': app,
            'tool': tool,
            'user': user,
            'job': job,
            'mapper': self
        }

        # 3. Evaluate entity properties that must be evaluated early, prior to combining
        evaluated_entities = self.evaluate_early(entity_list, context)

        # 4. Combine entity requirements
        combined_entity = self.combine_entities(evaluated_entities)

        context.update({
            'entity': combined_entity,
            'self': combined_entity
        })

        # 5. Evaluate remaining expressions after combining requirements
        evaluated = combined_entity.evaluate_late(context)

        context.update({
            'entity': evaluated,
            'self': evaluated
        })

        # 6. Find best matching destination
        destination = self.find_best_match(evaluated, self.destinations, context)

        # 7. Return destination with params
        if destination:
            destination = app.job_config.get_destination(destination.id)
            if evaluated.env:
                destination.env += [dict(name=k, value=v) for (k, v) in evaluated.env.items()]
            destination.params.update(evaluated.params or {})
            return destination
        else:
            from galaxy.jobs.mapper import JobMappingException
            raise JobMappingException(f"No destinations are available to fulfill request: {evaluated.id}")
