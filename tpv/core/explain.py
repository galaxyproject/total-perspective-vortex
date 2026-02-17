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
