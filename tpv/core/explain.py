from __future__ import annotations

import dataclasses
import io
from enum import Enum
from typing import TYPE_CHECKING, Any

from ruamel.yaml import YAML

if TYPE_CHECKING:
    from .entities import Destination, Entity


class ExplainPhase(Enum):
    CONFIG_LOADING = "Configuration Loading"
    ENTITY_MATCHING = "Entity Matching"
    ENTITY_COMBINING = "Entity Combining"
    RULE_EVALUATION = "Rule Evaluation"
    RESOURCE_EVALUATION = "Resource Evaluation"
    DESTINATION_MATCHING = "Destination Matching"
    DESTINATION_RANKING = "Destination Ranking"
    DESTINATION_EVALUATION = "Destination Evaluation"
    RESOURCE_POOLS = "Resource Pools"
    FINAL_RESULT = "Final Result"


@dataclasses.dataclass
class ExplainStep:
    phase: ExplainPhase
    message: str
    detail: str | None = None


class ExplainCollector:
    """Accumulates explain steps during a TPV scheduling decision."""

    CONTEXT_KEY = "__explain"

    def __init__(self) -> None:
        self.steps: list[ExplainStep] = []

    def add_step(
        self,
        phase: ExplainPhase,
        message: str,
        detail: str | None = None,
    ) -> None:
        self.steps.append(ExplainStep(phase=phase, message=message, detail=detail))

    @staticmethod
    def from_context(context: dict[str, Any]) -> "ExplainCollector" | None:
        """Retrieve the collector from a context dict, or None if not explaining."""
        return context.get(ExplainCollector.CONTEXT_KEY)

    @staticmethod
    def _abstract_destination_summary_step(destination_ids: list[str]) -> ExplainStep:
        count = len(destination_ids)
        noun = "destination" if count == 1 else "destinations"
        ids = ", ".join(destination_ids[:5])
        if count > 5:
            ids = f"{ids}, ... ({count - 5} more)"
        return ExplainStep(
            phase=ExplainPhase.DESTINATION_MATCHING,
            message=f"Skipped {count} abstract {noun}",
            detail=f"ids: {ids}",
        )

    @staticmethod
    def _destination_id_from_rejection_message(message: str) -> str:
        return message.split(": REJECTED", 1)[0]

    @staticmethod
    def _is_abstract_destination_rejection(step: ExplainStep) -> bool:
        return (
            step.phase == ExplainPhase.DESTINATION_MATCHING
            and step.detail == "destination is abstract"
            and ": REJECTED" in step.message
        )

    def _steps_for_output(self) -> list[ExplainStep]:
        """Prepare explain steps for display-oriented output."""
        abstract_destination_ids = [
            self._destination_id_from_rejection_message(step.message)
            for step in self.steps
            if self._is_abstract_destination_rejection(step)
        ]
        if not abstract_destination_ids:
            return self.steps

        output_steps: list[ExplainStep] = []
        summary_inserted = False
        for step in self.steps:
            if self._is_abstract_destination_rejection(step):
                if not summary_inserted:
                    output_steps.append(self._abstract_destination_summary_step(abstract_destination_ids))
                    summary_inserted = True
                continue
            output_steps.append(step)

        return output_steps

    def render(self) -> str:
        """Render the collected steps as structured, human-readable text."""
        buf = io.StringIO()
        buf.write("=" * 72 + "\n")
        buf.write("TPV SCHEDULING DECISION TRACE\n")
        buf.write("=" * 72 + "\n\n")

        current_phase = None
        step_num = 0
        for step in self._steps_for_output():
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
        data: dict[str, Any] = {"phases": {}}
        current_phase = None
        phase_steps: list[dict[str, Any]] = []

        for step in self._steps_for_output():
            if step.phase != current_phase:
                if current_phase is not None:
                    data["phases"][current_phase.value] = phase_steps
                current_phase = step.phase
                phase_steps = []
            step_data: dict[str, Any] = {"message": step.message}
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

    @staticmethod
    def match_failure_reason(dest: Destination, entity: Entity) -> str:
        """Produce a human-readable reason why a destination didn't match an entity."""
        if dest.abstract:
            return "destination is abstract"
        if (
            dest.max_accepted_cores is not None
            and entity.cores is not None
            and dest.max_accepted_cores < float(entity.cores)
        ):
            return f"cores {entity.cores} exceeds max_accepted_cores {dest.max_accepted_cores}"
        if dest.max_accepted_mem is not None and entity.mem is not None and dest.max_accepted_mem < float(entity.mem):
            return f"mem {entity.mem} exceeds max_accepted_mem {dest.max_accepted_mem}"
        if (
            dest.max_accepted_gpus is not None
            and entity.gpus is not None
            and dest.max_accepted_gpus < float(entity.gpus)
        ):
            return f"gpus {entity.gpus} exceeds max_accepted_gpus {dest.max_accepted_gpus}"
        if (
            dest.min_accepted_cores is not None
            and entity.cores is not None
            and dest.min_accepted_cores > float(entity.cores)
        ):
            return f"cores {entity.cores} below min_accepted_cores {dest.min_accepted_cores}"
        if dest.min_accepted_mem is not None and entity.mem is not None and dest.min_accepted_mem > float(entity.mem):
            return f"mem {entity.mem} below min_accepted_mem {dest.min_accepted_mem}"
        if (
            dest.min_accepted_gpus is not None
            and entity.gpus is not None
            and dest.min_accepted_gpus > float(entity.gpus)
        ):
            return f"gpus {entity.gpus} below min_accepted_gpus {dest.min_accepted_gpus}"
        if not entity.tpv_tags.match(dest.tpv_dest_tags):
            return f"tag mismatch - entity tags: {entity.tpv_tags}\n" f"dest tags: {dest.tpv_dest_tags}"
        return "unknown reason"
