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

        _render_section(buf, "Tools", config.tools)
        _render_section(buf, "Users", config.users)
        if config.roles:
            _render_section(buf, "Roles", config.roles)
        _render_section(buf, "Destinations", config.destinations)

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


_SKIP_FIELDS = {"id", "abstract"}


def _render_section(
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
        data = entity.model_dump(exclude_defaults=True)
        for key, value in data.items():
            if key in _SKIP_FIELDS:
                continue
            if key == "destination_name_override" and value == entity_id:
                continue
            if key == "scheduling":
                tag_data = {k: v for k, v in value.items() if v}
                if tag_data:
                    tag_parts = [f"{k}={v}" for k, v in tag_data.items()]
                    buf.write(f"    scheduling: {', '.join(tag_parts)}\n")
            elif key == "rules":
                buf.write(f"    rules: {len(value)} rule(s)\n")
            else:
                buf.write(f"    {key}: {value}\n")
        buf.write("\n")
