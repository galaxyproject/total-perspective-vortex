import io
import logging
from typing import Any, Dict, List, Optional

from ruamel.yaml import YAML

from tpv.core.loader import TPVConfigLoader

log = logging.getLogger(__name__)


class TPVConfigDumper:

    def __init__(self, config_files: List[str]):
        self.config_files = config_files
        self.loader: Optional[TPVConfigLoader] = None
        for config_file in config_files:
            self.loader = TPVConfigLoader.from_url_or_path(config_file, parent=self.loader)

    def dump(self, output_format: str = "text") -> str:
        if self.loader is None:
            return ""
        if output_format == "yaml":
            return self._render_yaml()
        return self._render_text()

    def _render_text(self) -> str:
        config = self.loader.config  # type: ignore[union-attr]
        buf = io.StringIO()
        buf.write("=" * 72 + "\n")
        buf.write("TPV MERGED CONFIGURATION\n")
        if self.config_files:
            buf.write("Sources (in load order):\n")
            for i, source in enumerate(self.config_files, 1):
                buf.write(f"  {i}. {source}\n")
        buf.write("=" * 72 + "\n\n")

        # Global
        buf.write("--- Global ---\n")
        gc = config.global_config
        if gc.default_inherits:
            buf.write(f"  default_inherits: {gc.default_inherits}\n")
        if gc.context:
            buf.write("  context:\n")
            for k, v in gc.context.items():
                buf.write(f"    {k}: {v}\n")
        buf.write("\n")

        # Tools
        _render_entity_section(buf, "Tools", config.tools)
        # Users
        _render_entity_section(buf, "Users", config.users)
        # Roles
        if config.roles:
            _render_entity_section(buf, "Roles", config.roles)
        # Destinations
        _render_destination_section(buf, "Destinations", config.destinations)

        buf.write("=" * 72 + "\n")
        return buf.getvalue()

    def _render_yaml(self) -> str:
        config = self.loader.config  # type: ignore[union-attr]
        data = config.model_dump()
        if self.config_files:
            data["_sources"] = self.config_files
        yaml = YAML()
        yaml.default_flow_style = False
        buf = io.StringIO()
        yaml.dump(data, buf)
        return buf.getvalue()

    @staticmethod
    def from_url_or_path(config_files: List[str]) -> "TPVConfigDumper":
        return TPVConfigDumper(config_files)


def _render_entity_section(
    buf: io.StringIO,
    title: str,
    entities: Dict[str, Any],
) -> None:
    if not entities:
        return
    buf.write(f"--- {title} ---\n")
    for entity_id, entity in entities.items():
        abstract_marker = " (abstract)" if entity.abstract else ""
        buf.write(f"  {entity_id}{abstract_marker}:\n")
        props = []
        if entity.cores is not None:
            props.append(f"cores={entity.cores}")
        if entity.mem is not None:
            props.append(f"mem={entity.mem}")
        if entity.gpus is not None:
            props.append(f"gpus={entity.gpus}")
        if props:
            buf.write(f"    {', '.join(props)}\n")
        tags = entity.tpv_tags
        tag_parts = []
        if tags.require:
            tag_parts.append(f"require={tags.require}")
        if tags.prefer:
            tag_parts.append(f"prefer={tags.prefer}")
        if tags.accept:
            tag_parts.append(f"accept={tags.accept}")
        if tags.reject:
            tag_parts.append(f"reject={tags.reject}")
        if tag_parts:
            buf.write(f"    scheduling: {', '.join(tag_parts)}\n")
        if hasattr(entity, "rules") and entity.rules:
            buf.write(f"    rules: {len(entity.rules)} rule(s)\n")
        buf.write("\n")


def _render_destination_section(
    buf: io.StringIO,
    title: str,
    destinations: Dict[str, Any],
) -> None:
    if not destinations:
        return
    buf.write(f"--- {title} ---\n")
    for dest_id, dest in destinations.items():
        abstract_marker = " (abstract)" if dest.abstract else ""
        buf.write(f"  {dest_id}{abstract_marker}:\n")
        if dest.runner:
            buf.write(f"    runner: {dest.runner}\n")
        capacity = []
        if dest.max_accepted_cores is not None:
            capacity.append(f"max_accepted_cores={dest.max_accepted_cores}")
        if dest.max_accepted_mem is not None:
            capacity.append(f"max_accepted_mem={dest.max_accepted_mem}")
        if dest.max_accepted_gpus is not None:
            capacity.append(f"max_accepted_gpus={dest.max_accepted_gpus}")
        if dest.min_accepted_cores is not None:
            capacity.append(f"min_accepted_cores={dest.min_accepted_cores}")
        if dest.min_accepted_mem is not None:
            capacity.append(f"min_accepted_mem={dest.min_accepted_mem}")
        if dest.min_accepted_gpus is not None:
            capacity.append(f"min_accepted_gpus={dest.min_accepted_gpus}")
        if capacity:
            buf.write(f"    {', '.join(capacity)}\n")
        tags = dest.tpv_dest_tags
        tag_parts = []
        if tags.require:
            tag_parts.append(f"require={tags.require}")
        if tags.prefer:
            tag_parts.append(f"prefer={tags.prefer}")
        if tags.accept:
            tag_parts.append(f"accept={tags.accept}")
        if tags.reject:
            tag_parts.append(f"reject={tags.reject}")
        if tag_parts:
            buf.write(f"    scheduling: {', '.join(tag_parts)}\n")
        if dest.rules:
            buf.write(f"    rules: {len(dest.rules)} rule(s)\n")
        buf.write("\n")
