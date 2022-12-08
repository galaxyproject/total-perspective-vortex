import logging

from .loader import TPVConfigLoader

log = logging.getLogger(__name__)


class TPVLintError(Exception):
    pass


class TPVConfigLinter(object):

    def __init__(self, url_or_path):
        self.url_or_path = url_or_path
        self.errors = []

    def lint(self):
        try:
            loader = TPVConfigLoader.from_url_or_path(self.url_or_path)
        except Exception as e:
            log.error(f"Linting failed due to syntax errors in yaml file: {e}")
            raise TPVLintError("Linting failed due to syntax error in yaml file: ") from e
        for destination in loader.destinations.values():
            if not destination.runner:
                self.errors.append(f"Destination '{destination.id}' does not define the runner parameter. "
                                   "The runner parameter is mandatory.")
        if self.errors:
            for e in self.errors:
                log.error(e)
            raise TPVLintError(f"The following errors occurred during linting: {self.errors}")

    @staticmethod
    def from_url_or_path(url_or_path: str):
        return TPVConfigLinter(url_or_path)
