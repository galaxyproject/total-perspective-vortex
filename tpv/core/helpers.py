try:
    from galaxy.tool_util.version import parse_version
except ImportError:
    # Fallback to an older `packaging` version when Galaxy < 23.1.
    # If Galaxy is < 23.1 you need to have `packaging` in <= 21.3
    from packaging.version import parse as parse_version

import operator
import random
import re
from collections.abc import Callable
from functools import reduce
from typing import Any

from galaxy import model
from galaxy.app import UniverseApplication
from galaxy.model import Dataset, HistoryDatasetAssociation, Job, JobToInputDatasetAssociation
from galaxy.model import User as GalaxyUser
from galaxy.tools import Tool as GalaxyTool

from tpv.core.entities import Destination, Entity
from tpv.core.resource_requirements import TPVResourceFieldName, extract_resource_requirements_from_tool

GIGABYTES = 1024.0**3

# Datatype extension suffixes that indicate a compressed dataset, e.g. `fastqsanger.gz`
COMPRESSED_EXTENSION_SUFFIXES = (".gz", ".bz2")

# Default multiplier applied to compressed input sizes to approximate their decompressed size
DEFAULT_COMPRESSION_FACTOR = 3.4


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


def __unique_input_datasets(
    datasets: list[JobToInputDatasetAssociation] | None,
) -> dict[int, HistoryDatasetAssociation]:
    # Galaxy records a `multiple="true"` data param under both `name` and `name1`, so the same
    # dataset can be associated with a job more than once. Key on the underlying dataset to
    # count each file only once, preserving the order the inputs were recorded in.
    unique_datasets: dict[int, HistoryDatasetAssociation] = {}
    for inp_ds in datasets or []:
        if inp_ds.dataset and inp_ds.dataset.dataset:
            unique_datasets.setdefault(inp_ds.dataset.dataset.id, inp_ds.dataset)
    return unique_datasets


def get_input_datasets(job: Job, param_name: str | None = None) -> list[HistoryDatasetAssociation]:
    """
    Return a job's input datasets, optionally restricted to a single tool parameter.

    Galaxy flattens data params into `job.input_datasets` by name: a single data param is
    recorded as `name`, a `multiple="true"` data param as `name`, `name1`..`nameN` (where
    `name` is an alias for `name1`), and a collection param as `name1`..`nameN`, one entry per
    dataset in the collection. Matching all of those, then deduplicating, yields each dataset
    of the parameter exactly once regardless of how it was supplied.

    Note that `param_name` is the fully prefixed parameter name, so a data param nested in a
    conditional is addressed as e.g. `cond|input`.
    """
    datasets = job.input_datasets
    if param_name is not None:
        pattern = re.compile(rf"^{re.escape(param_name)}\d*$")
        datasets = [inp_ds for inp_ds in datasets or [] if pattern.match(inp_ds.name or "")]
    return list(__unique_input_datasets(datasets).values())


def get_input_dataset(job: Job, param_name: str) -> HistoryDatasetAssociation | None:
    """
    Return the first input dataset recorded for a tool parameter, or None if the parameter has
    no input dataset (an unset optional param, for example).
    """
    datasets = get_input_datasets(job, param_name)
    return datasets[0] if datasets else None


def get_input_size(
    job: Job,
    param_name: str | None = None,
    estimate_uncompressed_size: bool = True,
    compression_factor: float = DEFAULT_COMPRESSION_FACTOR,
) -> float:
    """
    Return the total size in gigabytes of a job's input datasets.

    If `param_name` is given, only the datasets recorded for that tool parameter are totalled,
    otherwise all of the job's input datasets are. Compressed inputs are multiplied by
    `compression_factor` to estimate their uncompressed size, unless
    `estimate_uncompressed_size` is False.
    """
    total = 0.0
    for dataset in get_input_datasets(job, param_name):
        if not dataset.dataset:
            continue
        multiplier = 1.0
        if estimate_uncompressed_size and (dataset.extension or "").endswith(COMPRESSED_EXTENSION_SUFFIXES):
            multiplier = compression_factor
        total += get_dataset_size(dataset.dataset) * multiplier
    return total / GIGABYTES


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
        if i.dataset
    }
