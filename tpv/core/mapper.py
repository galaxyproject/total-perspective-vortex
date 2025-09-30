import functools
import logging
import re
from typing import Any, Dict, List, Mapping, Optional, TypeVar, cast

from galaxy.app import UniverseApplication
from galaxy.jobs import JobDestination, JobWrapper
from galaxy.jobs.mapper import JobNotReadyException
from galaxy.model import Job
from galaxy.model import User as GalaxyUser
from galaxy.tools import Tool as GalaxyTool

from .entities import (
    Destination,
    Entity,
    EntityWithRules,
    SchedulingTags,
    Tool,
    TryNextDestinationOrFail,
    TryNextDestinationOrWait,
)
from .loader import TPVConfigLoader

log = logging.getLogger(__name__)

EntityType = TypeVar("EntityType", bound=Entity)


class EntityToDestinationMapper(object):

    def __init__(self, loader: TPVConfigLoader):
        self.loader = loader
        self.config = loader.config
        self.destinations = self.config.destinations
        self.default_inherits = self.config.global_config.default_inherits
        self.global_context = self.config.global_config.context
        self.lookup_tool_regex = functools.lru_cache(maxsize=None)(self.__compile_tool_regex)
        self.inherit_matching_entities = functools.lru_cache(maxsize=None)(self.__inherit_matching_entities)

    def __compile_tool_regex(self, key: str) -> re.Pattern[str]:
        try:
            return re.compile(key)
        except re.error:
            log.error(f"Failed to compile regex: {key}")
            raise

    def _find_entities_matching_id(self, entity_list: dict[str, EntityType], entity_name: str) -> List[EntityType]:
        default_inherits = self.__get_default_inherits(entity_list)
        if default_inherits:
            matches = [default_inherits]
        else:
            matches = []
        for key in entity_list.keys():
            if self.lookup_tool_regex(key).match(entity_name):
                match = entity_list[key]
                if match.abstract:
                    from galaxy.jobs.mapper import JobMappingException

                    raise JobMappingException(
                        f"This entity is abstract and cannot be mapped : {match}"
                    )  # type: ignore[no-untyped-call]
                else:
                    matches.append(match)
        return matches

    def __inherit_matching_entities(self, entity_type: str, entity_name: str) -> Optional[EntityWithRules]:
        entity_list: Dict[str, EntityWithRules] = getattr(self.config, entity_type)
        matches = self._find_entities_matching_id(entity_list, entity_name)
        if matches:
            return self.inherit_entities(matches)
        else:
            return None

    def __get_default_inherits(self, entity_list: Mapping[str, EntityType]) -> Optional[EntityType]:
        if self.default_inherits:
            default_match = entity_list.get(self.default_inherits)
            if default_match:
                return default_match
        return None

    def __apply_default_destination_inheritance(self, entity_list: Dict[str, Destination]) -> List[Destination]:
        default_inherits = self.__get_default_inherits(entity_list)
        if default_inherits:
            return [self.inherit_entities([default_inherits, entity]) for entity in entity_list.values()]
        else:
            return list(entity_list.values())

    def inherit_entities(self, entities: List[EntityType]) -> EntityType:
        return functools.reduce(lambda a, b: b.inherit(a), entities)

    def combine_entities(self, entities: List[EntityType]) -> EntityType:
        return functools.reduce(lambda a, b: b.combine(a), entities)

    def rank(self, entity: Entity, destinations: List[Destination], context: Dict[str, Any]) -> List[Destination]:
        return entity.rank_destinations(destinations, context)

    def match_and_rank_destinations(
        self,
        entity: Entity,
        destinations: Dict[str, Destination],
        context: Dict[str, Any],
    ) -> List[Destination]:
        # At this point, the resource requirements (cores, mem, gpus) are unevaluated.
        # So temporarily evaluate them so we can match up with a destination.
        matches = [
            dest
            for dest in self.__apply_default_destination_inheritance(destinations)
            if dest.matches(entity.evaluate_resources(context), context)
        ]
        return self.rank(entity, matches, context)

    def to_galaxy_destination(self, destination: Destination) -> JobDestination:
        return JobDestination(
            id=destination.dest_name,
            tags=destination.handler_tags,
            runner=destination.runner,
            params=destination.params,
            env=destination.env,
            resubmit=list(destination.resubmit.values()),
        )  # type: ignore[no-untyped-call]

    def _find_matching_entities(self, tool: GalaxyTool, user: Optional[GalaxyUser]) -> List[EntityWithRules]:
        tool_entity = self.inherit_matching_entities("tools", tool.id)
        if not tool_entity:
            tool_entity = Tool(evaluator=self.loader, id=tool.id or "unknown_tool_id")

        tool_type_tag = f"tool_type_{tool.tool_type}"

        # Create a new accept list with the tool_type tag, preserving existing tags
        existing_accept = tool_entity.tpv_tags.accept or []
        new_accept = existing_accept.copy() if existing_accept else []
        if tool_type_tag not in new_accept:
            new_accept.append(tool_type_tag)

        tool_entity.tpv_tags = SchedulingTags(
            require=tool_entity.tpv_tags.require,
            prefer=tool_entity.tpv_tags.prefer,
            accept=new_accept,
            reject=tool_entity.tpv_tags.reject,
        )

        entity_list: List[EntityWithRules] = [tool_entity]

        if user:
            role_entities = (
                self.inherit_matching_entities("roles", role.name)
                for role in user.all_roles()  # type: ignore[no-untyped-call]
                if not role.deleted
            )
            # trim empty
            user_role_entities = (role for role in role_entities if role)
            user_role_entity = next(user_role_entities, None)
            if user_role_entity:
                entity_list += [user_role_entity]

            user_entity = self.inherit_matching_entities("users", user.email)
            if user_entity:
                entity_list += [user_entity]

        return entity_list

    def match_combine_evaluate_entities(
        self, context: Dict[str, Any], tool: GalaxyTool, user: Optional[GalaxyUser]
    ) -> EntityWithRules:
        # 1. Find the entities relevant to this job
        entity_list = self._find_matching_entities(tool, user)

        # 2. Combine entity requirements
        combined_entity = self.combine_entities(entity_list)
        context.update({"entity": combined_entity, "self": combined_entity})

        # 3. Evaluate rules only, so that all expressions are collapsed into a flat entity. The final
        #    values for expressions should be evaluated only after combining with the destination.
        evaluated_entity = combined_entity.evaluate_rules(context)
        context.update({"entity": evaluated_entity, "self": evaluated_entity})

        # Remove the rules as they've already been evaluated, and should not be re-evaluated when combining
        # with destinations
        evaluated_entity.rules = {}

        return evaluated_entity

    def map_to_destination(
        self,
        app: UniverseApplication,
        tool: GalaxyTool,
        user: Optional[GalaxyUser],
        job: Job,
        job_wrapper: Optional[JobWrapper] = None,
        resource_params: Optional[Dict[str, Any]] = None,
        workflow_invocation_uuid: Optional[str] = None,
    ) -> JobDestination:

        # 1. Create evaluation context - these are the common variables available within any code block
        context = {}
        context.update(self.global_context or {})
        context.update(
            {
                "app": app,
                "tool": tool,
                "user": user,
                "job": job,
                "job_wrapper": job_wrapper,
                "resource_params": resource_params,
                "workflow_invocation_uuid": workflow_invocation_uuid,
                "mapper": self,
            }
        )

        # 2. Find, combine and evaluate entities that match this tool and user
        evaluated_entity = self.match_combine_evaluate_entities(context, tool, user)

        # 3. Match and rank destinations that best match the combined entity
        ranked_dest_entities = self.match_and_rank_destinations(evaluated_entity, self.destinations, context)

        # 4. Fully combine entity with matching destinations
        if ranked_dest_entities:
            wait_exception_raised = False
            for d in ranked_dest_entities:
                try:  # An exception here signifies that a destination rule did not match
                    dest_combined_entity = d.combine(cast(Destination, evaluated_entity))
                    evaluated_destination = dest_combined_entity.evaluate(context)
                    # 5. Return the top-ranked destination that evaluates successfully
                    return self.to_galaxy_destination(evaluated_destination)
                except TryNextDestinationOrFail as ef:
                    log.exception(
                        f"Destination entity: {d} matched but could not fulfill requirements due to: {ef}."
                        " Trying next candidate..."
                    )
                except TryNextDestinationOrWait:
                    wait_exception_raised = True
            if wait_exception_raised:
                raise JobNotReadyException()  # type: ignore[no-untyped-call]

        # No matching destinations. Throw an exception
        from galaxy.jobs.mapper import JobMappingException

        raise JobMappingException(
            f"No destinations are available to fulfill request: {evaluated_entity.id}"
        )  # type: ignore[no-untyped-call]
