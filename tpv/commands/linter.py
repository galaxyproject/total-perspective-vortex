import logging
import re

from tpv.core.loader import TPVConfigLoader

log = logging.getLogger(__name__)


class TPVLintError(Exception):
    pass


class TPVConfigLinter(object):

    def __init__(self, url_or_path):
        self.url_or_path = url_or_path
        self.warnings = []
        self.errors = []

    def lint(self):
        try:
            loader = TPVConfigLoader.from_url_or_path(self.url_or_path)
        except Exception as e:
            log.error(f"Linting failed due to syntax errors in yaml file: {e}")
            raise TPVLintError("Linting failed due to syntax errors in yaml file: ") from e
        self.lint_tools(loader)
        self.lint_destinations(loader)
        self.print_errors_and_warnings()

    def lint_tools(self, loader):
        default_inherits = loader.global_settings.get('default_inherits')
        for tool_regex, tool in loader.tools.items():
            try:
                re.compile(tool_regex)
            except re.error:
                self.errors.append(f"Failed to compile regex: {tool_regex}")
            if default_inherits == tool.id:
                self.warnings.append(
                    f"The tool named: {default_inherits} is marked globally as the tool to inherit from "
                    "by default. You may want to mark it as abstract if it is not an actual tool and it "
                    "will be excluded from scheduling decisions.")

    def lint_destinations(self, loader):
        default_inherits = loader.global_settings.get('default_inherits')
        for destination in loader.config.destinations.values():
            if not destination.runner and not destination.abstract:
                self.errors.append(f"Destination '{destination.id}' does not define the runner parameter. "
                                   "The runner parameter is mandatory.")
            if ((destination.cores and not destination.max_accepted_cores) or
                    (destination.mem and not destination.max_accepted_mem) or
                    (destination.gpus and not destination.max_accepted_gpus)):
                self.errors.append(
                    f"The destination named: {destination.id} defines the cores/mem/gpus property instead of "
                    f"max_accepted_cores/mem/gpus. This is probably an error. If you're migrating from an older "
                    f"version of TPV, the destination properties for cores/mem/gpus have been superseded by the "
                    f"max_accepted_cores/mem/gpus property. Simply renaming them will give you the same functionality.")
            if default_inherits == destination.id:
                self.warnings.append(
                    f"The destination named: {default_inherits} is marked globally as the destination to inherit from "
                    "by default. You may want to mark it as abstract if it is not meant to be dispatched to, and it "
                    "will be excluded from scheduling decisions.")

    def print_errors_and_warnings(self):
        if self.warnings:
            for w in self.warnings:
                log.warning(w)
        if self.errors:
            for e in self.errors:
                log.error(e)
            raise TPVLintError(f"The following errors occurred during linting: {self.errors}")

    @staticmethod
    def from_url_or_path(url_or_path: str):
        return TPVConfigLinter(url_or_path)
