import logging
import os

from galaxy.util.watcher import get_watcher
from vortex.core.loader import VortexConfigLoader
from vortex.core.mapper import ResourceToDestinationMapper

log = logging.getLogger(__name__)


ACTIVE_DESTINATION_MAPPER = None


def load_destination_mapper(vortex_config_files, reload=False):
    log.info(f"{'re' if reload else ''}loading vortex rules from: {vortex_config_files}")
    loader = None
    for vortex_config_file in vortex_config_files:
        current_loader = VortexConfigLoader.from_url_or_path(vortex_config_file)
        if loader:
            loader.merge_loader(current_loader)
        else:
            loader = current_loader
    return ResourceToDestinationMapper(loader)


def reload_destination_mapper(path=None):
    global ACTIVE_DESTINATION_MAPPER
    ACTIVE_DESTINATION_MAPPER = load_destination_mapper(path, reload=True)


def setup_destination_mapper(app, vortex_config_files):
    mapper = load_destination_mapper(vortex_config_files)
    job_rule_watcher = get_watcher(app.config, 'watch_job_rules', monitor_what_str='job rules')
    for vortex_config_file in vortex_config_files:
        if os.path.isfile(vortex_config_file):
            log.info(f"Watching for changes in file: {vortex_config_file}")
            job_rule_watcher.watch_file(vortex_config_file, callback=reload_destination_mapper)
            job_rule_watcher.start()
    return mapper


def map_tool_to_destination(app, job, tool, user, vortex_config_files):
    global ACTIVE_DESTINATION_MAPPER
    if not ACTIVE_DESTINATION_MAPPER:
        ACTIVE_DESTINATION_MAPPER = setup_destination_mapper(app, vortex_config_files)
    return ACTIVE_DESTINATION_MAPPER.map_to_destination(app, tool, user, job)
