import ast
import inspect
import logging
import os
import re
import tempfile
import textwrap
from typing import Annotated, Any, Dict, List, Set, Tuple, Type, get_args, get_origin

import mypy.api
from jinja2 import Environment, FileSystemLoader
from pydantic.fields import FieldInfo

from tpv.core.entities import Entity, TPVFieldMetadata
from tpv.core.loader import TPVConfigLoader

log = logging.getLogger(__name__)


# Optional mapping for known "weird" but serializable types
SERIALIZABLE_TYPE_MAP = {
    "ruamel.yaml.scalarfloat.ScalarFloat": "float",
    "ruamel.yaml.scalarint.ScalarInt": "int",
    "ruamel.yaml.scalarbool.ScalarBoolean": "bool",
    "ruamel.yaml.scalarstring.ScalarString": "str",
}


def get_serializable_type_str(return_type: Any) -> str:
    """
    Given a type/class, convert it to a nice string for serializing into source code.
    Handles built-in types and known wrapper types like ruamel.yaml's scalar types.
    """
    if inspect.isclass(return_type):
        module = return_type.__module__
        qualname = return_type.__qualname__
        full_name = f"{module}.{qualname}"

        # Handle known type replacements
        if full_name in SERIALIZABLE_TYPE_MAP:
            return SERIALIZABLE_TYPE_MAP[full_name]

        if module == "builtins":
            return return_type.__name__.replace("NoneType", "None")

    # Fallback: prettify the string output
    return str(return_type).replace("NoneType", "None").replace("<class '", "").replace("'>", "")


def get_return_type_str(field_info: FieldInfo, value: Any) -> str:
    """
    Attempt to convert the field's annotation into a nice string for a function return.
    Example: Annotated[Optional[int], TPVFieldMetadata(...)] -> Optional[int]
    """
    annotation = field_info.annotation  # e.g. Annotated[Optional[int], TPVFieldMetadata()]
    origin = get_origin(annotation)
    args = get_args(annotation)

    # If a return type is explicitly defined on the Entity field, use that.
    metadata_list = getattr(field_info, "metadata", [])
    return_type: Any
    if any(isinstance(m, TPVFieldMetadata) and m.return_type is not None for m in metadata_list):
        return_type = [
            m.return_type for m in metadata_list if isinstance(m, TPVFieldMetadata) and m.return_type is not None
        ][0]
    # if it's a complex_type (e.g. env, params), use the leaf value to infer type (usually string)
    elif any(isinstance(m, TPVFieldMetadata) and getattr(m, "complex_property", False) for m in metadata_list):
        return_type = type(value)
    # If it's Annotated[<something>, <metadata>], args[0] is the underlying type
    elif origin is type(Annotated[Any, []]):  # or: if origin is Annotated:
        return_type = args[0]
    else:
        return_type = annotation

    return get_serializable_type_str(return_type)


def add_return_to_last_expr(code: str) -> str:
    """Return code in which the final *expression* is rewritten as a
    `return …`. Also handle multi-line expressions."""
    code = str(code)  # make sure type has been converted to string
    src = textwrap.dedent(code).rstrip()
    tree = ast.parse(src, mode="exec")
    if not tree.body or not isinstance(tree.body[-1], ast.Expr):
        return src  # nothing to transform

    last = tree.body[-1]
    expr_src = ast.get_source_segment(src, last)
    # Drop the lines that make up the expression we’re replacing
    lines = src.splitlines()
    if last.end_lineno:
        for i in range(last.lineno - 1, last.end_lineno):
            lines[i] = ""
    # Insert the return at the position where the expression started
    lines.insert(last.lineno - 1, f"return {expr_src}")
    return "\n".join(l for l in lines if l)


def render_optional_union(type_names: List[str]) -> str:
    cleaned = [t for t in type_names if t != "None"]

    if not cleaned:
        return "Optional[Any]"
    elif len(cleaned) == 1:
        return f"Optional[{cleaned[0]}]"
    else:
        union_part = " | ".join(sorted(cleaned))
        return f"Optional[{union_part}]"


def gather_all_evaluable_code(
    loader: TPVConfigLoader,
) -> Tuple[Dict[str, Any], List[dict[str, str]]]:
    """
    Returns a list of all context vars and a list of (func_name, code_block) for all evaluable fields
    from all entities in the TPVConfig.
    """
    context_vars_container: Dict[str, Any] = {}
    code_blocks = []

    # Gather from top-level groups
    for tool_id, tool in loader.config.tools.items():
        code_blocks.extend(gather_fields_from_entity(loader, context_vars_container, tool, f"tool_{tool_id}"))

    for user_id, user in loader.config.users.items():
        code_blocks.extend(gather_fields_from_entity(loader, context_vars_container, user, f"user_{user_id}"))

    for role_id, role in loader.config.roles.items():
        code_blocks.extend(gather_fields_from_entity(loader, context_vars_container, role, f"role_{role_id}"))

    for dest_id, dest in loader.config.destinations.items():
        code_blocks.extend(gather_fields_from_entity(loader, context_vars_container, dest, f"dest_{dest_id}"))

    context_vars_serializable = {
        key: render_optional_union([get_serializable_type_str(typ) for typ in val])
        for key, val in context_vars_container.items()
    }
    return context_vars_serializable, code_blocks


def infer_context_var_type(
    context_vars_container: Dict[str, Set[Type[Any]]],
    entity_context_vars: Dict[str, Any],
) -> None:
    for var_name, var_val in entity_context_vars.items():
        possible_types = context_vars_container.get(var_name, set())
        if type(var_val) not in possible_types:
            possible_types.add(type(var_val))
        context_vars_container[var_name] = possible_types


def gather_fields_from_entity(
    loader: TPVConfigLoader,
    context_vars_container: Dict[str, Any],
    entity: Entity,
    path: str,
) -> List[dict[str, str]]:
    """
    Return a list of dicts, each dict with:
      {
        'func_name': str,
        'code': str,
        'return_type': str
      }
    """
    context_vars = getattr(entity, "context") or {}
    infer_context_var_type(context_vars_container, context_vars)
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
                    getattr(m, "complex_property", False) for m in metadata_list if isinstance(m, TPVFieldMetadata)
                )

                eval_as_f_string = (
                    True
                    if is_complex
                    else any(
                        getattr(m, "eval_as_f_string", False) for m in metadata_list if isinstance(m, TPVFieldMetadata)
                    )
                )

                def add_code_block(block_name: str, value: Any) -> None:
                    safe_name = slugify(f"{path}_{block_name}" if path else block_name)

                    # Derive a return type string from the Entity type annotation
                    return_type = get_return_type_str(field_info, value)

                    code_snippets.append(
                        {
                            "func_name": safe_name,
                            "code": (f"f'''{value}'''".replace("\n", "") if eval_as_f_string else value),
                            "return_type": return_type,
                        }
                    )

                if is_complex:
                    loader.process_complex_property(field_name, value, {}, lambda n, v, c: add_code_block(n, v))
                else:
                    add_code_block(field_name, value)

    if hasattr(entity, "rules"):
        for rule_id, rule in entity.rules.items():
            code_snippets.extend(gather_fields_from_entity(loader, context_vars_container, rule, f"{path}_{rule_id}"))

    return code_snippets


def type_check_code(loader: TPVConfigLoader, preserve_temp_code: bool) -> tuple[int, List[str], str]:
    """
    1) Gather all evaluable code blocks from the loaded TPVConfig.
    2) Render them to a single .py file using Jinja2.
    3) Run mypy and record errors if any.
    """
    # 1. Gather code blocks
    context_vars, code_blocks = gather_all_evaluable_code(loader)
    if not code_blocks:
        # Nothing to check
        return (0, [], "")

    # 2. Render with Jinja2
    current_dir = os.path.dirname(os.path.abspath(__file__))
    env = Environment(loader=FileSystemLoader(current_dir))
    env.filters["returnify"] = add_return_to_last_expr
    template = env.get_template("type_check_template.j2")
    rendered_code = template.render(context_vars=context_vars, code_blocks=code_blocks)

    # 3. Write the rendered code to a temp file and run mypy
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=not preserve_temp_code) as tmp_file:
        tmp_filename = tmp_file.name
        tmp_file.write(rendered_code)
        tmp_file.flush()

        mypy_args = [
            # "--no-incremental", # https://stackoverflow.com/a/65223004/10971151
            tmp_filename,
        ]
        stdout, stderr, exit_code = mypy.api.run(mypy_args)
        if exit_code != 0:
            # the last line in both stdout and stderr and useless, so always remove those
            errors = stdout.strip().split("\n")[:-1]
            errors.extend(stderr.strip().split("\n")[:-1])
            return exit_code, errors, tmp_filename
        else:
            return (0, [], "")


def slugify(value: str) -> str:
    """
    Convert value into a nice function-safe slug:
      - Lowercase
      - Replace non-alphanumeric or underscore runs with _
    """
    slug = value.lower()
    slug = re.sub(r"[^a-z0-9_]+", "_", slug)
    return slug
