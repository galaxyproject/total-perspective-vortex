import dataclasses
import io
from enum import Enum
from typing import Any, Dict, List, Optional

from ruamel.yaml import YAML


class ExplainPhase(Enum):
    CONFIG_LOADING = "Configuration Loading"
    ENTITY_MATCHING = "Entity Matching"
    ENTITY_COMBINING = "Entity Combining"
    RULE_EVALUATION = "Rule Evaluation"
    RESOURCE_EVALUATION = "Resource Evaluation"
    DESTINATION_MATCHING = "Destination Matching"
    DESTINATION_RANKING = "Destination Ranking"
    DESTINATION_EVALUATION = "Destination Evaluation"
    FINAL_RESULT = "Final Result"


@dataclasses.dataclass
class ExplainStep:
    phase: ExplainPhase
    message: str
    detail: Optional[str] = None


class ExplainCollector:
    """Accumulates explain steps during a TPV scheduling decision."""

    CONTEXT_KEY = "__explain"

    def __init__(self) -> None:
        self.steps: List[ExplainStep] = []

    def add_step(
        self,
        phase: ExplainPhase,
        message: str,
        detail: Optional[str] = None,
    ) -> None:
        self.steps.append(ExplainStep(phase=phase, message=message, detail=detail))

    @staticmethod
    def from_context(context: Dict[str, Any]) -> Optional["ExplainCollector"]:
        """Retrieve the collector from a context dict, or None if not explaining."""
        return context.get(ExplainCollector.CONTEXT_KEY)

    def render(self) -> str:
        """Render the collected steps as structured, human-readable text."""
        buf = io.StringIO()
        buf.write("=" * 72 + "\n")
        buf.write("TPV SCHEDULING DECISION TRACE\n")
        buf.write("=" * 72 + "\n\n")

        current_phase = None
        step_num = 0
        for step in self.steps:
            if step.phase != current_phase:
                current_phase = step.phase
                buf.write(f"--- {current_phase.value} ---\n")
            step_num += 1
            buf.write(f"  [{step_num}] {step.message}\n")
            if step.detail:
                for line in step.detail.strip().splitlines():
                    buf.write(f"        {line}\n")
            buf.write("\n")

        buf.write("=" * 72 + "\n")
        return buf.getvalue()

    def render_yaml(self) -> str:
        """Render the collected steps as YAML."""
        data: Dict[str, Any] = {"phases": {}}
        current_phase = None
        phase_steps: List[Dict[str, Any]] = []

        for step in self.steps:
            if step.phase != current_phase:
                if current_phase is not None:
                    data["phases"][current_phase.value] = phase_steps
                current_phase = step.phase
                phase_steps = []
            step_data: Dict[str, Any] = {"message": step.message}
            if step.detail:
                step_data["detail"] = step.detail
            phase_steps.append(step_data)

        if current_phase is not None:
            data["phases"][current_phase.value] = phase_steps

        yaml = YAML()
        yaml.default_flow_style = False
        buf = io.StringIO()
        yaml.dump(data, buf)
        return buf.getvalue()


class MergedConfigRenderer:
    """Renders a merged TPV configuration for display."""

    @staticmethod
    def render_text(config: Any, sources: List[str]) -> str:
        """Render merged config as structured text."""
        buf = io.StringIO()
        buf.write("=" * 72 + "\n")
        buf.write("TPV MERGED CONFIGURATION\n")
        if sources:
            buf.write("Sources (in load order):\n")
            for i, source in enumerate(sources, 1):
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
        _render_entity_section(buf, "Tools", config.tools, show_runner=False)
        # Users
        _render_entity_section(buf, "Users", config.users, show_runner=False)
        # Roles
        if config.roles:
            _render_entity_section(buf, "Roles", config.roles, show_runner=False)
        # Destinations
        _render_destination_section(buf, "Destinations", config.destinations)

        buf.write("=" * 72 + "\n")
        return buf.getvalue()

    @staticmethod
    def render_yaml(config: Any, sources: List[str]) -> str:
        """Render merged config as YAML."""
        data = config.model_dump()
        if sources:
            data["_sources"] = sources
        yaml = YAML()
        yaml.default_flow_style = False
        buf = io.StringIO()
        yaml.dump(data, buf)
        return buf.getvalue()


def _render_entity_section(
    buf: io.StringIO,
    title: str,
    entities: Dict[str, Any],
    show_runner: bool = False,
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
