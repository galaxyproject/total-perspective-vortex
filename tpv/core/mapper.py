import functools
import logging
import re
from collections.abc import Mapping
from typing import Any, TypeVar, cast

from cachetools import Cache, cached
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
    Role,
    SchedulingTags,
    Tool,
    TryNextDestinationOrFail,
    TryNextDestinationOrWait,
    User,
)
from .explain import ExplainCollector, ExplainPhase
from .loader import TPVConfigLoader
from .resource_requirements import extract_resource_requirements_from_tool

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
        self._cache_inherit_matching_entities: Any = Cache(maxsize=0)

        def _cache_key_ignore_context(
            context: dict[str, Any], entity_type: type[EntityType], entity_field: str, entity_name: str
        ) -> tuple[type[EntityType], str, str]:
            # ignore context in the key
            return (entity_type, entity_field, entity_name)

        self.inherit_matching_entities = cached(self._cache_inherit_matching_entities, key=_cache_key_ignore_context)(
            self.__inherit_matching_entities
        )

    def __compile_tool_regex(self, key: str) -> re.Pattern[str]:
        try:
            return re.compile(key)
        except re.error:
            log.error(f"Failed to compile regex: {key}")
            raise

    def _get_common_inherits(
        self, context: Mapping[str, Any], entity_list: dict[str, EntityType], entity_type: type[EntityType]
    ) -> list[EntityType]:
        """Gets inherited values common to all entities (because destinations do not inherit regex matches)"""
        matches: list[EntityType] = []
        default_inherits = self.__get_default_inherits(entity_list)
        if default_inherits:
            matches += [default_inherits]
        env_inherits = self.__get_environment_inherits(entity_type, context)
        if env_inherits:
            matches += [env_inherits]
        return matches

    def _find_entities_matching_id(
        self,
        context: Mapping[str, Any],
        entity_list: dict[str, EntityType],
        entity_name: str,
        entity_type: type[EntityType],
    ) -> list[EntityType]:
        matches = self._get_common_inherits(context, entity_list, entity_type)
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

    def __inherit_matching_entities(
        self, context: dict[str, Any], entity_type: type[EntityType], entity_field: str, entity_name: str
    ) -> EntityType | None:
        entity_list: dict[str, EntityType] = getattr(self.config, entity_field)
        matches: list[EntityType] = self._find_entities_matching_id(context, entity_list, entity_name, entity_type)
        if matches:
            return self.inherit_entities(matches)
        else:
            return None

    def __get_environment_inherits(
        self, entity_type: type[EntityType], context: Mapping[str, Any]
    ) -> EntityType | None:
        if issubclass(entity_type, Tool) and context.get("tool"):
            galaxy_tool: GalaxyTool = context["tool"]
            resource_fields = extract_resource_requirements_from_tool(galaxy_tool)
            tpv_tool = Tool(
                evaluator=self.loader,
                id=f"tool_provided_resources_{getattr(galaxy_tool.dynamic_tool, 'uuid', galaxy_tool.id)}",
                scheduling=SchedulingTags(accept=[f"tool_type_{galaxy_tool.tool_type}"]),
                **resource_fields,
            )
            return cast(EntityType, tpv_tool)
        if issubclass(entity_type, Destination):
            # Secure default: user-defined tools must be explicitly accepted by a destination.
            tpv_destination = Destination(
                evaluator=self.loader,
                id="tool_type_secure_defaults",
                scheduling=SchedulingTags(reject=["tool_type_user_defined"]),
            )
            return cast(EntityType, tpv_destination)
        return None

    def __get_default_inherits(self, entity_list: Mapping[str, EntityType]) -> EntityType | None:
        if self.default_inherits:
            default_match = entity_list.get(self.default_inherits)
            if default_match:
                return default_match
        return None

    def __apply_default_destination_inheritance(
        self, entity_list: dict[str, Destination], context: Mapping[str, Any]
    ) -> list[Destination]:
        inherited_defaults = self._get_common_inherits(context, entity_list, Destination)
        if inherited_defaults:
            return [self.inherit_entities([*inherited_defaults, entity]) for entity in entity_list.values()]
        return list(entity_list.values())

    def inherit_entities(self, entities: list[EntityType]) -> EntityType:
        return functools.reduce(lambda a, b: b.inherit(a), entities)

    def combine_entities(self, entities: list[EntityType]) -> EntityType:
        return functools.reduce(lambda a, b: b.combine(a), entities)

    def rank(self, entity: Entity, destinations: list[Destination], context: dict[str, Any]) -> list[Destination]:
        return entity.rank_destinations(destinations, context)

    @staticmethod
    def _explain_match_failure(dest: Destination, entity: Entity) -> str:
        """Produce a human-readable reason why a destination didn't match an entity."""
        if dest.abstract:
            return "destination is abstract"
        if (
            dest.max_accepted_cores is not None
            and entity.cores is not None
            and dest.max_accepted_cores < float(entity.cores)
        ):
            return f"cores {entity.cores} exceeds max_accepted_cores {dest.max_accepted_cores}"
        if dest.max_accepted_mem is not None and entity.mem is not None and dest.max_accepted_mem < float(entity.mem):
            return f"mem {entity.mem} exceeds max_accepted_mem {dest.max_accepted_mem}"
        if (
            dest.max_accepted_gpus is not None
            and entity.gpus is not None
            and dest.max_accepted_gpus < float(entity.gpus)
        ):
            return f"gpus {entity.gpus} exceeds max_accepted_gpus {dest.max_accepted_gpus}"
        if (
            dest.min_accepted_cores is not None
            and entity.cores is not None
            and dest.min_accepted_cores > float(entity.cores)
        ):
            return f"cores {entity.cores} below min_accepted_cores {dest.min_accepted_cores}"
        if dest.min_accepted_mem is not None and entity.mem is not None and dest.min_accepted_mem > float(entity.mem):
            return f"mem {entity.mem} below min_accepted_mem {dest.min_accepted_mem}"
        if (
            dest.min_accepted_gpus is not None
            and entity.gpus is not None
            and dest.min_accepted_gpus > float(entity.gpus)
        ):
            return f"gpus {entity.gpus} below min_accepted_gpus {dest.min_accepted_gpus}"
        if not entity.tpv_tags.match(dest.tpv_dest_tags):
            return (
                f"tag mismatch - entity requires {entity.tpv_tags.require}, rejects {entity.tpv_tags.reject} "
                f"dest tags are {list(dest.tpv_dest_tags.all_tag_values())}"
            )
        return "unknown reason"

    def match_and_rank_destinations(
        self,
        entity: Entity,
        destinations: dict[str, Destination],
        context: dict[str, Any],
    ) -> list[Destination]:
        explain = ExplainCollector.from_context(context)
        # At this point, the resource requirements (cores, mem, gpus) are unevaluated.
        # So temporarily evaluate them so we can match up with a destination.
        evaluated_entity = entity.evaluate_resources(context)

        if explain:
            explain.add_step(
                ExplainPhase.RESOURCE_EVALUATION,
                "Evaluated resource expressions",
                f"cores={evaluated_entity.cores}, mem={evaluated_entity.mem}, gpus={evaluated_entity.gpus}",
            )

        all_dests = self.__apply_default_destination_inheritance(destinations)
        matches = []
        for dest in all_dests:
            if dest.matches(evaluated_entity, context):
                matches.append(dest)
                if explain:
                    capacity_parts = []
                    if dest.max_accepted_cores is not None:
                        capacity_parts.append(f"max_cores={dest.max_accepted_cores}")
                    if dest.max_accepted_mem is not None:
                        capacity_parts.append(f"max_mem={dest.max_accepted_mem}")
                    if dest.max_accepted_gpus is not None:
                        capacity_parts.append(f"max_gpus={dest.max_accepted_gpus}")
                    explain.add_step(
                        ExplainPhase.DESTINATION_MATCHING,
                        f"{dest.id}: MATCHED",
                        f"capacity: {', '.join(capacity_parts)}" if capacity_parts else None,
                    )
            else:
                if explain:
                    reason = self._explain_match_failure(dest, evaluated_entity)
                    explain.add_step(
                        ExplainPhase.DESTINATION_MATCHING,
                        f"{dest.id}: REJECTED",
                        reason,
                    )

        ranked = self.rank(entity, matches, context)
        if explain:
            for i, d in enumerate(ranked):
                score = d.score(entity)
                explain.add_step(
                    ExplainPhase.DESTINATION_RANKING,
                    f"#{i + 1} {d.id} (score: {score})",
                )
        return ranked

    def to_galaxy_destination(self, destination: Destination) -> JobDestination:
        return JobDestination(
            id=destination.dest_name,
            tags=destination.handler_tags,
            runner=destination.runner,
            params=destination.params,
            env=destination.env,
            resubmit=list(destination.resubmit.values()),
        )  # type: ignore[no-untyped-call]

    def _find_matching_entities(
        self, context: dict[str, Any], tool: GalaxyTool, user: GalaxyUser | None
    ) -> list[EntityWithRules]:
        explain = ExplainCollector.from_context(context)
        # Prefer tool uuid if available, we don't want user defined tools to be able to hijack another tools' rules.
        if tool.dynamic_tool:
            tool_id = f"{tool.tool_type}-{tool.dynamic_tool.uuid}"
        else:
            tool_id = tool.id or "unknown_tool_id"
        tool_entity = self.inherit_matching_entities(context, Tool, "tools", tool_id)

        if not tool_entity:
            tool_entity = Tool(evaluator=self.loader, id=tool_id)
            if explain:
                explain.add_step(
                    ExplainPhase.ENTITY_MATCHING,
                    f"Tool '{tool_id}': no explicit match, using default",
                )
        else:
            if explain:
                explain.add_step(
                    ExplainPhase.ENTITY_MATCHING,
                    f"Tool '{tool_id}': matched entity '{tool_entity.id}'",
                )

        entity_list: list[EntityWithRules] = [tool_entity]

        if user:
            role_entities = (
                self.inherit_matching_entities(context, Role, "roles", role.name)
                for role in user.all_roles()  # type: ignore[no-untyped-call]
                if not role.deleted
            )
            # trim empty
            user_role_entities = (role for role in role_entities if role)
            user_role_entity = next(user_role_entities, None)
            if user_role_entity:
                entity_list += [user_role_entity]
                if explain:
                    explain.add_step(
                        ExplainPhase.ENTITY_MATCHING,
                        f"Role: matched entity '{user_role_entity.id}'",
                    )
            else:
                if explain:
                    explain.add_step(ExplainPhase.ENTITY_MATCHING, "No role entities matched")

            user_entity = self.inherit_matching_entities(context, User, "users", user.email)
            if user_entity:
                entity_list += [user_entity]
                if explain:
                    explain.add_step(
                        ExplainPhase.ENTITY_MATCHING,
                        f"User '{user.email}': matched entity '{user_entity.id}'",
                    )
            else:
                if explain:
                    explain.add_step(
                        ExplainPhase.ENTITY_MATCHING,
                        f"User '{user.email}': no matching entity",
                    )
        else:
            if explain:
                explain.add_step(ExplainPhase.ENTITY_MATCHING, "No user specified")

        return entity_list

    def match_combine_evaluate_entities(
        self, context: dict[str, Any], tool: GalaxyTool, user: GalaxyUser | None
    ) -> EntityWithRules:
        explain = ExplainCollector.from_context(context)
        # 1. Find the entities relevant to this job
        entity_list = self._find_matching_entities(context, tool, user)

        # 2. Combine entity requirements
        combined_entity = self.combine_entities(entity_list)
        context.update({"entity": combined_entity, "self": combined_entity})

        if explain:
            entity_names = [f"{type(e).__name__}({e.id})" for e in entity_list]
            explain.add_step(
                ExplainPhase.ENTITY_COMBINING,
                f"Combining entities: {' + '.join(entity_names)}",
                f"cores={combined_entity.cores}, mem={combined_entity.mem}, gpus={combined_entity.gpus}\n"
                f"scheduling: require={combined_entity.tpv_tags.require}, "
                f"prefer={combined_entity.tpv_tags.prefer}, "
                f"reject={combined_entity.tpv_tags.reject}",
            )

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
        user: GalaxyUser | None,
        job: Job,
        job_wrapper: JobWrapper | None = None,
        resource_params: dict[str, Any] | None = None,
        workflow_invocation_uuid: str | None = None,
        explain_collector: ExplainCollector | None = None,
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

        # Inject the explain collector into the context
        if explain_collector:
            context[ExplainCollector.CONTEXT_KEY] = explain_collector

        # 2. Find, combine and evaluate entities that match this tool and user
        evaluated_entity = self.match_combine_evaluate_entities(context, tool, user)

        # 3. Match and rank destinations that best match the combined entity
        ranked_dest_entities = self.match_and_rank_destinations(evaluated_entity, self.destinations, context)

        explain = ExplainCollector.from_context(context)

        # 4. Fully combine entity with matching destinations
        if ranked_dest_entities:
            wait_exception_raised = False
            for d in ranked_dest_entities:
                try:  # An exception here signifies that a destination rule did not match
                    if explain:
                        explain.add_step(
                            ExplainPhase.DESTINATION_EVALUATION,
                            f"Evaluating destination '{d.id}'",
                        )
                    dest_combined_entity = d.combine(cast(Destination, evaluated_entity))
                    evaluated_destination = dest_combined_entity.evaluate(context)
                    # 5. Return the top-ranked destination that evaluates successfully
                    if explain:
                        explain.add_step(
                            ExplainPhase.FINAL_RESULT,
                            f"Destination: {evaluated_destination.dest_name}",
                            f"runner: {evaluated_destination.runner}\n"
                            f"cores: {evaluated_destination.cores}, mem: {evaluated_destination.mem}, "
                            f"gpus: {evaluated_destination.gpus}\n"
                            f"params: {evaluated_destination.params}\n"
                            f"env: {evaluated_destination.env}",
                        )
                    return self.to_galaxy_destination(evaluated_destination)
                except TryNextDestinationOrFail as ef:
                    if explain:
                        explain.add_step(
                            ExplainPhase.DESTINATION_EVALUATION,
                            f"Destination '{d.id}' failed: {ef}, trying next...",
                        )
                    log.exception(
                        f"Destination entity: {d} matched but could not fulfill requirements due to: {ef}."
                        " Trying next candidate..."
                    )
                except TryNextDestinationOrWait as ew:
                    if explain:
                        explain.add_step(
                            ExplainPhase.DESTINATION_EVALUATION,
                            f"Destination '{d.id}' deferred: {ew}, trying next...",
                        )
                    wait_exception_raised = True
            if wait_exception_raised:
                if explain:
                    explain.add_step(
                        ExplainPhase.FINAL_RESULT,
                        "All matching destinations deferred (job not ready)",
                    )
                raise JobNotReadyException()  # type: ignore[no-untyped-call]

        # No matching destinations. Throw an exception
        from galaxy.jobs.mapper import JobMappingException

        if explain:
            explain.add_step(
                ExplainPhase.FINAL_RESULT,
                f"No destinations are available to fulfill request: {evaluated_entity.id}",
            )
        raise JobMappingException(
            f"No destinations are available to fulfill request: {evaluated_entity.id}"
        )  # type: ignore[no-untyped-call]
