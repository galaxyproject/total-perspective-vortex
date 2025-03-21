import logging
import os
import threading
from collections import defaultdict
from typing import List, Union

from galaxy.util import listify
from galaxy.util.watcher import get_watcher

from tpv.core.loader import TPVConfigLoader
from tpv.core.mapper import EntityToDestinationMapper

log = logging.getLogger(__name__)


ACTIVE_DESTINATION_MAPPERS = {}
DESTINATION_MAPPER_LOCK = threading.Lock()
WATCHERS_BY_CONFIG_FILE = {}
REFERRERS_BY_CONFIG_FILE = defaultdict(dict)


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
    global WATCHERS_BY_CONFIG_FILE
    global REFERRERS_BY_CONFIG_FILE
    mapper = load_destination_mapper(tpv_config_files)

    for tpv_config_file in tpv_config_files:
        if os.path.isfile(tpv_config_file):
            # adjust for tempfile handling on Darwin
            tpv_config_real_path = os.path.realpath(tpv_config_file)
            log.info(
                f"Watching for changes in file: {tpv_config_real_path} via referrer: {referrer}"
            )
            # there can't be two watchers on the same file
            watcher = WATCHERS_BY_CONFIG_FILE.get(tpv_config_real_path)
            if not watcher:
                watcher = get_watcher(
                    app.config, "watch_job_rules", monitor_what_str="job rules"
                )

            def reload_destination_mapper(path=None):
                # reload all config files when one file changes to preserve order of loading the files
                global ACTIVE_DESTINATION_MAPPERS
                # watchdog on darwin notifies only once per file, so reload all mappers that refer to this file
                for referrer, config_files in REFERRERS_BY_CONFIG_FILE[
                    tpv_config_real_path
                ].items():
                    ACTIVE_DESTINATION_MAPPERS[referrer] = load_destination_mapper(
                        config_files, reload=True
                    )

            WATCHERS_BY_CONFIG_FILE[tpv_config_real_path] = watcher
            REFERRERS_BY_CONFIG_FILE[tpv_config_real_path][referrer] = tpv_config_files
            watcher.watch_file(tpv_config_real_path, callback=reload_destination_mapper)
            watcher.start()

    return mapper


def lock_and_load_mapper(app, referrer, tpv_config_files):
    global ACTIVE_DESTINATION_MAPPERS
    destination_mapper = ACTIVE_DESTINATION_MAPPERS.get(referrer)
    if not destination_mapper:
        # Try again with a lock
        # We need a lock to avoid thundering herd problems and to serialize access to WATCHERS_BY_CONFIG_FILE
        with DESTINATION_MAPPER_LOCK:
            destination_mapper = ACTIVE_DESTINATION_MAPPERS.get(referrer)
            # still null with the lock - must be the first time
            if not destination_mapper:
                destination_mapper = setup_destination_mapper(
                    app, referrer, tpv_config_files
                )
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
    return destination_mapper.map_to_destination(
        app, tool, user, job, job_wrapper, resource_params, workflow_invocation_uuid
    )
