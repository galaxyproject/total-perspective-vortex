from typing import Literal, TypedDict, Union, get_args

from galaxy.tools import Tool as GalaxyTool

ResourceType = Literal[
    "cores_min",
    "cores_max",
    "ram_min",
    "ram_max",
    "tmpdir_min",
    "tmpdir_max",
    "cuda_version_min",
    "cuda_compute_capability",
    "gpu_memory_min",
    "cuda_device_count_min",
    "cuda_device_count_max",
    "shm_size",
]
VALID_RESOURCE_TYPES = get_args(ResourceType)

TPVResourceFieldName = Literal["cores", "max_cores", "mem", "max_mem", "gpus", "max_gpus"]


class TPVResourceFields(TypedDict, total=False):
    """Type definition for TPV entity resource fields that can be extracted from Galaxy tools."""

    cores: Union[int, float]
    max_cores: Union[int, float]
    mem: Union[int, float]
    max_mem: Union[int, float]
    gpus: int
    max_gpus: int


def extract_resource_requirements_from_tool(
    galaxy_tool: GalaxyTool,
) -> TPVResourceFields:
    """
    Extract resource requirements from a Galaxy tool and convert them to TPV entity fields.

    Args:
        galaxy_tool: Galaxy tool object with resource_requirements attribute

    Returns:
        TPVResourceFields containing extracted resource requirements as TPV entity fields
    """
    if not getattr(galaxy_tool, "resource_requirements", []):
        return {}

    # Mapping from Galaxy resource types to TPV entity fields
    # It seems that for tool entities you can only specify `cores`, not a range ?
    resource_mapping = {
        "cores_min": "cores",
        "cores_max": "max_cores",
        "ram_min": "mem",
        "ram_max": "max_mem",
        "cuda_device_count_min": "gpus",
        "cuda_device_count_max": "max_gpus",
        # Note: TPV doesn't have direct equivalents for tmpdir, cuda_version, etc.
        # These could be added later or handled via custom rules
    }

    extracted: TPVResourceFields = {}

    for resource_req in galaxy_tool.resource_requirements:
        if resource_req.resource_type in resource_mapping:
            tpv_field = resource_mapping[resource_req.resource_type]
            try:
                # Extract the value. runtime should be passed if we want to evaluate expressions,
                # but interface on galaxy side not yet implemented.
                value = resource_req.get_value()
                if value is not None:
                    extracted[tpv_field] = value  # type: ignore[literal-required]
            except NotImplementedError:
                # Skip expression evaluation for now
                continue

    return extracted
