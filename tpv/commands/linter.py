import logging
import re
from typing import Any, List, Optional, Tuple

from pydantic import BaseModel

from tpv.commands import mypychecker
from tpv.core.entities import Entity
from tpv.core.loader import TPVConfigLoader

log = logging.getLogger(__name__)


# Warning codes:
# T101: default inheritance not marked abstract
# T102: entity specifies cores without memory
# T103: mypy error
# T104: unexpected field


class TPVLintError(Exception):
    pass


class TPVConfigLinter(object):

    def __init__(self, url_or_path: List[str], ignore: Optional[List[str]], preserve_temp_code: bool):
        self.url_or_path: List[str] = url_or_path
        self.ignore: List[str] = ignore or []
        self.preserve_temp_code = preserve_temp_code
        self.warnings: List[Tuple[str, str]] = []
        self.errors: List[str | Tuple[str, str]] = []
        self.loader: Optional[TPVConfigLoader] = None

    def load_config(self) -> None:
        loader = None
        for tpv_config in self.url_or_path:
            try:
                loader = TPVConfigLoader.from_url_or_path(tpv_config, parent=loader)
                self.loader = loader
            except Exception as e:
                log.error(f"Linting failed due to syntax errors in yaml file: {e}")
                raise TPVLintError("Linting failed due to syntax errors in yaml file: ") from e

    def add_warning(self, code: str, message: str) -> None:
        if code not in self.ignore:
            self.warnings.append((code, message))

    def add_entity_warning(self, entity: Entity, code: str, message: str) -> None:
        if not entity.should_skip_qa(code):
            self.add_warning(code, message)

    def lint(self) -> None:
        if self.loader is None:
            self.load_config()
        if self.loader is not None:  # satisfy mypy
            self.lint_extra_fields(self.loader)
            self.lint_code(self.loader)
            self.lint_tools(self.loader)
            self.lint_destinations(self.loader)
            self.print_errors_and_warnings()

    def lint_extra_fields(self, loader: TPVConfigLoader) -> None:
        self.check_for_extra_fields_recurse(loader.config, "")

    def check_for_extra_fields_recurse(
        self,
        obj: Any,
        path: str,
    ) -> None:
        if isinstance(obj, BaseModel):
            # Check if there are extra fields
            extras = getattr(obj, "__pydantic_extra__", None)
            if extras:
                for key in extras:
                    self.add_warning(
                        "T104",
                        f"Unexpected field '{path}.{key}' - make sure the field is nested"
                        " correctly or manually silence warning",
                    )

            # Recurse into fields
            for field_name, _ in obj.model_fields.items():
                value = getattr(obj, field_name)
                self.check_for_extra_fields_recurse(value, f"{path}.{field_name}")

        elif isinstance(obj, list):
            for idx, item in enumerate(obj):
                self.check_for_extra_fields_recurse(item, f"{path}.{str(idx)}")

        elif isinstance(obj, dict):
            for key, value in obj.items():
                self.check_for_extra_fields_recurse(value, f"{path}.{str(key)}")

    def lint_code(self, loader: TPVConfigLoader) -> None:
        """
        Gather code blocks from the loader, render them into a .py file with Jinja2,
        run mypy, record errors if any.
        """
        exit_code, errors, _ = mypychecker.type_check_code(loader, self.preserve_temp_code)
        if exit_code != 0:
            for err in errors:
                self.add_warning("T103", err)

    def lint_tools(self, loader: TPVConfigLoader) -> None:
        default_inherits = loader.config.global_config.default_inherits
        for tool_regex, tool in loader.config.tools.items():
            try:
                re.compile(tool_regex)
            except re.error:
                self.errors.append(f"Failed to compile regex: {tool_regex}")
            if default_inherits == tool.id and not tool.abstract:
                self.add_entity_warning(
                    tool,
                    "T101",
                    f"The tool named: {default_inherits} is marked globally as the tool to inherit from "
                    "by default. You may want to mark it as abstract if it is not an actual tool and it "
                    "will be excluded from scheduling decisions.",
                )
            if tool.cores and not tool.mem:
                self.add_entity_warning(
                    tool,
                    "T102",
                    f"The tool named: {tool_regex} sets `cores` but not `mem`. This can lead to "
                    "unexpected memory usage since memory is typically a multiplier of cores.",
                )

    def lint_destinations(self, loader: TPVConfigLoader) -> None:
        default_inherits = loader.config.global_config.default_inherits
        for destination in loader.config.destinations.values():
            if not destination.runner and not destination.abstract:
                self.errors.append(
                    f"Destination '{destination.id}' does not define the runner parameter. "
                    "The runner parameter is mandatory."
                )
            if (
                (destination.cores and not destination.max_accepted_cores)
                or (destination.mem and not destination.max_accepted_mem)
                or (destination.gpus and not destination.max_accepted_gpus)
            ):
                self.errors.append(
                    f"The destination named: {destination.id} defines the cores/mem/gpus property instead of "
                    f"max_accepted_cores/mem/gpus. This is probably an error. If you're migrating from an older "
                    f"version of TPV, the destination properties for cores/mem/gpus have been superseded by the "
                    f"max_accepted_cores/mem/gpus property. Simply renaming them will give you the same functionality."
                )
            if default_inherits == destination.id and not destination.abstract:
                self.add_entity_warning(
                    destination,
                    "T101",
                    f"The destination named: {default_inherits} is marked globally as the destination to inherit from "
                    "by default. You may want to mark it as abstract if it is not meant to be dispatched to, and it "
                    "will be excluded from scheduling decisions.",
                )

    def print_errors_and_warnings(self) -> None:
        if self.warnings:
            for code, message in self.warnings:
                log.warning(f"{code}: {message}")
        if self.errors:
            for e in self.errors:
                log.error(e)
            raise TPVLintError(f"The following errors occurred during linting: {self.errors}")

    @staticmethod
    def from_url_or_path(
        url_or_path: List[str],
        ignore: Optional[List[str]] = None,
        preserve_temp_code: bool = False,
    ) -> "TPVConfigLinter":
        return TPVConfigLinter(url_or_path, ignore=ignore, preserve_temp_code=preserve_temp_code)
