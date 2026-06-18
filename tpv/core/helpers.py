try:
    from galaxy.tool_util.version import parse_version
except ImportError:
    # Fallback to an older `packaging` version when Galaxy < 23.1.
    # If Galaxy is < 23.1 you need to have `packaging` in <= 21.3
    from packaging.version import parse as parse_version

import copy
import logging
import operator
import random
from collections.abc import Callable
from functools import reduce
from typing import Any

from galaxy import model
from galaxy.app import UniverseApplication
from galaxy.jobs.mapper import JobMappingException, JobNotReadyException
from galaxy.model import Dataset, Job, JobToInputDatasetAssociation
from galaxy.model import User as GalaxyUser
from galaxy.tools import Tool as GalaxyTool

from tpv.core.entities import Destination, Entity
from tpv.core.resource_pool import (
    NORMAL,
    OVERSIZE,
    Budget,
    ResourceUsage,
    StoreUnavailable,
    terminal_job_ids,
)
from tpv.core.resource_requirements import TPVResourceFieldName, extract_resource_requirements_from_tool

log = logging.getLogger(__name__)

GIGABYTES = 1024.0**3


def get_dataset_size(dataset: Dataset) -> float:
    # calculate_size would mark file_size column as dirty
    # and may have unintended consequences
    return float(dataset.get_size(nice_size=False, calculate_size=False))


def sum_total(prev: float, current: float) -> float:
    return prev + current


def calculate_dataset_total(
    datasets: list[JobToInputDatasetAssociation] | None,
) -> float:
    if datasets:
        unique_datasets = {inp_ds.dataset.dataset.id: inp_ds.dataset.dataset for inp_ds in datasets if inp_ds.dataset}
        return reduce(sum_total, map(get_dataset_size, unique_datasets.values()), 0.0)
    else:
        return 0.0


def input_size(job: Job) -> float:
    return calculate_dataset_total(job.input_datasets) / GIGABYTES


def weighted_random_sampling(destinations: list[Destination]) -> list[Destination]:
    if not destinations:
        return []
    has_explicit_weight = any(d.params and "weight" in d.params for d in destinations)
    if not has_explicit_weight:
        return random.sample(destinations, k=len(destinations))
    rankings = [(d.params.get("weight", 1) if d.params else 1) for d in destinations]
    return random.choices(destinations, weights=rankings, k=len(destinations))


def __get_keys_from_dict(dl: Any, keys_list: list[str]) -> None:
    # This function builds a list using the keys from nested dictionaries
    # (copied from galaxyproject/galaxy lib/galaxy/jobs/dynamic_tool_destination.py)
    if isinstance(dl, dict):
        keys_list.extend(dl.keys())
        for x in dl.values():
            __get_keys_from_dict(x, keys_list)
    elif isinstance(dl, list):
        for x in dl:
            __get_keys_from_dict(x, keys_list)


def job_args_match(job: Job, app: UniverseApplication, args: dict[str, Any] | None) -> bool:
    # Check whether a dictionary of arguments matches a job's parameters.  This code is
    # from galaxyproject/galaxy lib/galaxy/jobs/dynamic_tool_destination.py
    if not args or not isinstance(args, dict):
        return False
    options = job.get_param_values(app)  # type: ignore[no-untyped-call]
    matched = True
    # check if the args in the config file are available
    for arg in args:
        arg_dict = {arg: args[arg]}
        arg_keys_list: list[str] = []
        __get_keys_from_dict(arg_dict, arg_keys_list)
        try:
            options_value = reduce(dict.__getitem__, arg_keys_list, options)
            arg_value = reduce(dict.__getitem__, arg_keys_list, arg_dict)
            if arg_value != options_value:
                matched = False
        except KeyError:
            matched = False
    return matched


def concurrent_job_count_for_tool(
    app: UniverseApplication, tool: GalaxyTool, user: GalaxyUser | None = None
) -> int:  # requires galaxy version >= 21.09
    # Match all tools, regardless of version. For example, a tool id such as "toolshed/repos/iuc/fastqc/0.1.0+galaxy1"
    # is turned into "toolshed/repos/iuc/fastqc/" and a LIKE query is performed on the tool_id column.
    tool_id = tool.id or "unknown_tool_id"
    tool_id_base = "/".join(tool_id.split("/")[:-1]) + "/" if "/" in tool_id else tool_id
    query = app.model.context.query(model.Job.id)
    if user:
        query = query.filter(model.Job.table.c.user_id == user.id)
    query = query.filter(model.Job.table.c.state.in_(["queued", "running"]))
    if "/" in tool_id_base:
        query = query.filter(model.Job.table.c.tool_id.like(f"{tool_id_base}%"))
    else:
        query = query.filter(model.Job.table.c.tool_id == tool.id)
    return query.count()


def tag_values_match(
    entity: Entity, match_tag_values: list[str] | None = None, exclude_tag_values: list[str] | None = None
) -> bool:
    # Return true if an entity has require/prefer/accept tags in the match_tags_values list
    # and no require/prefer/accept tags in the exclude_tag_values list
    match_tag_values = match_tag_values or []
    exclude_tag_values = exclude_tag_values or []
    return all([any(entity.tpv_tags.filter(tag_value=tag_value)) for tag_value in match_tag_values]) and not any(
        [any(entity.tpv_tags.filter(tag_value=tag_value)) for tag_value in exclude_tag_values]
    )


def __compare_tool_versions(
    versionA: str | None,
    versionB: str | None,
    comparator: Callable[[Any, Any], bool],
) -> bool | None:
    if versionA is None or versionB is None:
        return None
    return comparator(parse_version(versionA), parse_version(versionB))


def tool_version_eq(tool: GalaxyTool, version: str | None) -> bool | None:
    return __compare_tool_versions(tool.version, version, operator.eq)


def tool_version_lte(tool: GalaxyTool, version: str | None) -> bool | None:
    return __compare_tool_versions(tool.version, version, operator.le)


def tool_version_lt(tool: GalaxyTool, version: str | None) -> bool | None:
    return __compare_tool_versions(tool.version, version, operator.lt)


def tool_version_gte(tool: GalaxyTool, version: str | None) -> bool | None:
    return __compare_tool_versions(tool.version, version, operator.ge)


def tool_version_gt(tool: GalaxyTool, version: str | None) -> bool | None:
    return __compare_tool_versions(tool.version, version, operator.gt)


def get_tool_resource_field(tool: GalaxyTool, field_name: TPVResourceFieldName) -> int | float | None:
    resource_fields = extract_resource_requirements_from_tool(tool)
    return resource_fields.get(field_name)


def get_dataset_attributes(
    datasets: list[JobToInputDatasetAssociation] | None,
) -> dict[int, dict[str, Any]]:
    # Return a dictionary of dataset ids and their object store ids
    # and file sizes in bytes for all input datasets in a job
    return {
        i.dataset.dataset.id: {
            "object_store_id": i.dataset.dataset.object_store_id,
            "size": get_dataset_size(i.dataset.dataset),
        }
        for i in datasets or {}
    }


def _evaluate_request(entity: Entity, context: dict[str, Any]) -> ResourceUsage:
    # Resolve the entity's (possibly symbolic) cores/mem/gpus to clamped numbers. Pool rules
    # run during rule evaluation, before resources are finalised, so e.g. ``mem: cores * 3``
    # is still a string here. Use a shallow copy of the context so we don't pollute the live
    # evaluation (evaluate_resources sets cores/mem/gpus keys on the context it is given).
    evaluated = entity.evaluate_resources(copy.copy(context))
    return ResourceUsage(
        cores=float(evaluated.cores or 0),
        mem=float(evaluated.mem or 0),
        gpus=float(evaluated.gpus or 0),
    )


def _exceeds_budget(req: ResourceUsage, budget: Budget) -> bool:
    return (
        (budget.cores is not None and req.cores > budget.cores)
        or (budget.mem is not None and req.mem > budget.mem)
        or (budget.gpus is not None and req.gpus > budget.gpus)
    )


def enforce_resource_pool(context: dict[str, Any], name: str = "default") -> None:
    """Enforce a per-user resource pool for the job currently being mapped.

    Call this from a TPV rule's ``execute`` block, passing the evaluation ``context``::

        rules:
          - id: enforce_default_pool
            if: entity.cores or entity.mem or entity.gpus
            execute: |
              helpers.enforce_resource_pool(context, name="default")

    The user's aggregate allocation across their active jobs is tracked in the configured
    allocation store (see ``global.resource_pools``). If admitting this job would exceed the
    pool budget the job is deferred (``JobNotReadyException``); if the job is *oversize* (its
    own request exceeds the budget) it is admitted only up to ``oversize.max_concurrent`` and
    otherwise deferred, or failed (``JobMappingException``) when oversize jobs are not allowed
    or the request is beyond the ``hard_max_*`` ceiling. If the store is unreachable the job
    is deferred (fail-closed), never silently admitted.
    """
    mapper = context["mapper"]
    manager = getattr(mapper, "resource_pools", None)
    if manager is None:
        return  # resource pools not configured for this deployment; no-op
    pool = manager.pool(name)
    if pool is None:
        raise JobMappingException(  # type: ignore[no-untyped-call]
            f"Resource pool '{name}' is not defined in the TPV configuration"
        )

    user = context.get("user")
    if user is None:
        return  # anonymous jobs are not governed by per-user pools

    app = context["app"]
    job = context["job"]
    entity = context["entity"]

    budget = manager.provider.budget_for(user, name)
    req = _evaluate_request(entity, context)

    is_oversize = _exceeds_budget(req, budget)
    if is_oversize:
        ceiling = Budget(
            cores=pool.oversize.hard_max_cores,
            mem=pool.oversize.hard_max_mem,
            gpus=pool.oversize.hard_max_gpus,
        )
        if pool.oversize.max_concurrent <= 0 or _exceeds_budget(req, ceiling):
            raise JobMappingException(  # type: ignore[no-untyped-call]
                f"This job requests cores={req.cores}, mem={req.mem}, gpus={req.gpus}, which "
                f"exceeds your '{name}' resource pool allocation and can never be scheduled."
            )
    kind = OVERSIZE if is_oversize else NORMAL

    store = manager.store
    try:
        ledger_ids = set(store.read(name, user.id).keys())
        drop = terminal_job_ids(app.model.context, ledger_ids)
        admitted = store.admit(
            name,
            user.id,
            job.id,
            req,
            kind=kind,
            budget=budget,
            max_oversize=pool.oversize.max_concurrent,
            reserve_pool=pool.oversize.reserve_pool,
            drop_job_ids=drop,
        )
    except StoreUnavailable:
        log.warning("Resource pool '%s' store is unavailable; deferring job (fail-closed)", name)
        raise JobNotReadyException()  # type: ignore[no-untyped-call]

    if not admitted:
        raise JobNotReadyException()  # type: ignore[no-untyped-call]
