import logging
import os
from typing import List, Union

from galaxy.util import listify
from galaxy.util.watcher import get_watcher
from tpv.core.loader import TPVConfigLoader
from tpv.core.mapper import EntityToDestinationMapper
import threading

log = logging.getLogger(__name__)


ACTIVE_DESTINATION_MAPPERS = {}
DESTINATION_MAPPER_LOCK = threading.Lock()
CONFIG_WATCHERS = {}


def load_destination_mapper(tpv_config_files: Union[List[str], str], reload=False):
    tpv_config_files = listify(tpv_config_files)
    log.info(f"{'re' if reload else ''}loading tpv rules from: {tpv_config_files}")
    loader = None
    for tpv_config_file in tpv_config_files:
        current_loader = TPVConfigLoader.from_url_or_path(tpv_config_file)
        if loader:
            loader.merge_loader(current_loader)
        else:
            loader = current_loader
    return EntityToDestinationMapper(loader)


def setup_destination_mapper(app, referrer, tpv_config_files: Union[List[str], str]):
    mapper = load_destination_mapper(tpv_config_files)

    def reload_destination_mapper(path=None):
        # reload all config files when one file changes to preserve order of loading the files
        global ACTIVE_DESTINATION_MAPPERS
        ACTIVE_DESTINATION_MAPPERS[referrer] = load_destination_mapper(tpv_config_files, reload=True)

    for tpv_config_file in tpv_config_files:
        if os.path.isfile(tpv_config_file):
            log.info(f"Watching for changes in file: {tpv_config_file} via referrer: {referrer}")
            entry_name = f"{referrer}{tpv_config_file}"
            CONFIG_WATCHERS[entry_name] = CONFIG_WATCHERS.get(
                entry_name
            ) or get_watcher(
                app.config, "watch_job_rules", monitor_what_str="job rules"
            )
            CONFIG_WATCHERS[entry_name].watch_file(
                os.path.realpath(tpv_config_file), callback=reload_destination_mapper
            )
            CONFIG_WATCHERS[entry_name].start()
    return mapper


def lock_and_load_mapper(app, referrer, tpv_config_files):
    global ACTIVE_DESTINATION_MAPPERS
    destination_mapper = ACTIVE_DESTINATION_MAPPERS.get(referrer)
    if not destination_mapper:
        # Try again with a lock
        # Technically, this should work without a lock, but having a lock heads off any thundering herd
        # problems on handler restarts.
        with DESTINATION_MAPPER_LOCK:
            destination_mapper = ACTIVE_DESTINATION_MAPPERS.get(referrer)
            # still null with the lock - must be the first time
            if not destination_mapper:
                destination_mapper = setup_destination_mapper(app, referrer, tpv_config_files)
                ACTIVE_DESTINATION_MAPPERS[referrer] = destination_mapper
    return destination_mapper


def map_tool_to_destination(
    app,
    job,
    tool,
    user,
    tpv_config_files: Union[List[str], str],
    # the destination referring to the TPV dynamic destination, usually named "tpv_dispatcher"
    referrer="tpv_dispatcher",
    job_wrapper=None,
    resource_params=None,
    workflow_invocation_uuid=None,
):
    destination_mapper = lock_and_load_mapper(app, referrer, tpv_config_files)
    return destination_mapper.map_to_destination(app, tool, user, job, job_wrapper, resource_params,
                                                 workflow_invocation_uuid)
