import logging
import os
import re
import tempfile
from typing import Annotated, Any, List, Tuple, get_args, get_origin

import mypy.api
from jinja2 import Environment, FileSystemLoader
from pydantic import Field

from tpv.core.entities import Entity, TPVFieldMetadata

log = logging.getLogger(__name__)


def get_return_type_str(field_info: Field) -> str:
    """
    Attempt to convert the field's annotation into a nice string for a function return.
    Example: Annotated[Optional[int], TPVFieldMetadata(...)] -> Optional[int]
    """
    annotation = (
        field_info.annotation
    )  # e.g. Annotated[Optional[int], TPVFieldMetadata()]
    origin = get_origin(annotation)
    args = get_args(annotation)

    # If a return type is explicitly defined on the Entity field, use that.
    metadata_list = getattr(field_info, "metadata", [])
    if any(
        isinstance(m, TPVFieldMetadata) and getattr(m, "return_type", False)
        for m in metadata_list
    ):
        return_type = [
            getattr(m, "return_type", False)
            for m in metadata_list
            if isinstance(m, TPVFieldMetadata)
        ][0]
        return str(return_type)

    # If it's Annotated[<something>, <metadata>], args[0] is the underlying type
    if origin is type(Annotated[Any, []]):  # or: if origin is Annotated:
        underlying = args[0]
        return str(underlying).replace("NoneType", "None")

    # Otherwise, it might be Union or something else. Just return str(annotation).
    # e.g. Union[int, str], or Optional[str].
    return str(annotation).replace("NoneType", "None")


def gather_all_evaluable_code(tpv_config) -> List[Tuple[str, str]]:
    """
    Returns a list of (func_name, code_block) for all evaluable fields
    from all entities in the TPVConfig.
    """
    code_blocks = []

    # Gather from top-level groups
    for tool_id, tool in tpv_config.tools.items():
        code_blocks.extend(gather_fields_from_entity(f"tool_{tool_id}", tool))

    for user_id, user in tpv_config.users.items():
        code_blocks.extend(gather_fields_from_entity(f"user_{user_id}", user))

    for role_id, role in tpv_config.roles.items():
        code_blocks.extend(gather_fields_from_entity(f"role_{role_id}", role))

    for dest_id, dest in tpv_config.destinations.items():
        code_blocks.extend(gather_fields_from_entity(f"dest_{dest_id}", dest))

    return code_blocks


def gather_fields_from_entity(path: str, entity: Entity) -> List[dict]:
    """
    Return a list of dicts, each dict with:
      {
        'func_name': str,
        'code': str,
        'complex_property': bool
      }
    """
    code_snippets = []
    fields_dict = getattr(entity, "model_fields")

    for field_name, field_info in fields_dict.items():
        # field_info.metadata is typically a tuple
        metadata_list = getattr(field_info, "metadata", [])
        if any(isinstance(m, TPVFieldMetadata) for m in metadata_list):
            value = getattr(entity, field_name, None)
            if value:
                # Find out if it's "complex" from the actual metadata
                # e.g., the first item or check specifically for `complex_property=True`
                is_complex = any(
                    getattr(m, "complex_property", False)
                    for m in metadata_list
                    if isinstance(m, TPVFieldMetadata)
                )
                safe_name = slugify(f"{path}_{field_name}" if path else field_name)

                # Derive a return type string from the Entity type annotation
                return_type = get_return_type_str(field_info)

                code_snippets.append(
                    {
                        "func_name": safe_name,
                        "code": value,
                        "complex_property": is_complex,
                        "return_type": return_type,
                    }
                )
    return code_snippets


def type_check_code(loader):
    """
    1) Gather all evaluable code blocks from the loaded TPVConfig.
    2) Render them to a single .py file using Jinja2.
    3) Run mypy and record errors if any.
    """
    # 1. Gather code blocks
    tpv_config = loader.config  # This is a TPVConfig
    code_blocks = gather_all_evaluable_code(tpv_config)
    if not code_blocks:
        # Nothing to check
        return (None, None, None)

    # 2. Render with Jinja2
    current_dir = os.path.dirname(os.path.abspath(__file__))
    env = Environment(loader=FileSystemLoader(current_dir))
    template = env.get_template("type_check_template.j2")
    rendered_code = template.render(code_blocks=code_blocks)

    # 3. Write the rendered code to a temp file and run mypy
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tmp_file:
        tmp_filename = tmp_file.name
        tmp_file.write(rendered_code)

        mypy_args = [
            "--config-file",
            os.path.join(current_dir, "mypy.ini"),
            tmp_filename,
        ]
        stdout, stderr, exit_code = mypy.api.run(mypy_args)
        # stdout += f"############ {mypy_args}"
        return stdout, stderr, tmp_filename


def slugify(value: str) -> str:
    """
    Convert value into a nice function-safe slug:
      - Lowercase
      - Replace non-alphanumeric or underscore runs with _
    """
    slug = value.lower()
    slug = re.sub(r"[^a-z0-9_]+", "_", slug)
    return slug
