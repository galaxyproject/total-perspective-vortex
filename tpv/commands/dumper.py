import io
import logging
from typing import Any

from ruamel.yaml import YAML

from tpv.core.loader import TPVConfigLoader

log = logging.getLogger(__name__)


class TPVConfigDumper:

    _SKIP_FIELDS = {"id", "abstract"}

    def __init__(self, config_files: list[str]):
        self.config_files = config_files
        self.loader: TPVConfigLoader | None = None
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

        self._render_section(buf, "Tools", config.tools)
        self._render_section(buf, "Users", config.users)
        if config.roles:
            self._render_section(buf, "Roles", config.roles)
        self._render_section(buf, "Destinations", config.destinations)

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
    def _write_value(buf: io.StringIO, key: str, value: Any, indent: str) -> None:
        text = str(value)
        if "\n" in text:
            buf.write(f"{indent}{key}: |\n")
            for line in text.splitlines():
                buf.write(f"{indent}  {line}\n")
        else:
            buf.write(f"{indent}{key}: {value}\n")

    @classmethod
    def _render_section(
        cls,
        buf: io.StringIO,
        title: str,
        entities: dict[str, Any],
    ) -> None:
        if not entities:
            return
        buf.write(f"--- {title} ---\n")
        for entity_id, entity in entities.items():
            abstract_marker = " (abstract)" if entity.abstract else ""
            buf.write(f"  {entity_id}{abstract_marker}:\n")
            data = entity.model_dump(exclude_defaults=True)
            for key, value in data.items():
                if key in cls._SKIP_FIELDS:
                    continue
                if key == "destination_name_override" and value == entity_id:
                    continue
                if key == "scheduling":
                    tag_data = {k: v for k, v in value.items() if v}
                    if tag_data:
                        tag_parts = [f"{k}={v}" for k, v in tag_data.items()]
                        buf.write(f"    scheduling: {', '.join(tag_parts)}\n")
                elif key == "rules":
                    buf.write("    rules:\n")
                    for rule_id, rule_data in value.items():
                        condition = rule_data.get("if", "")
                        effects = {k: v for k, v in rule_data.items() if k not in ("id", "if")}
                        condition_str = str(condition) if condition else ""
                        if condition_str and "\n" not in condition_str:
                            buf.write(f"      {rule_id} [if: {condition_str}]\n")
                        else:
                            buf.write(f"      {rule_id}\n")
                            if condition_str:
                                cls._write_value(buf, "if", condition_str, "        ")
                        for ek, ev in effects.items():
                            cls._write_value(buf, ek, ev, "        ")
                else:
                    cls._write_value(buf, key, value, "    ")
            buf.write("\n")

    @staticmethod
    def from_url_or_path(config_files: list[str]) -> "TPVConfigDumper":
        return TPVConfigDumper(config_files)
