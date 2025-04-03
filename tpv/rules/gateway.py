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

JOB_YAML_CONFIG_TYPE = Union[List[Union[str, dict]], str, dict]


def load_destination_mapper(tpv_configs: JOB_YAML_CONFIG_TYPE, reload=False):
    tpv_configs = listify(tpv_configs)
    log.info(f"{'re' if reload else ''}loading tpv rules from: {tpv_configs}")
    loader = None
    for tpv_config in tpv_configs:
        if isinstance(tpv_config, str):
            current_loader = TPVConfigLoader.from_url_or_path(tpv_config, parent=loader)
        else:
            # it is a raw config already
            current_loader = TPVConfigLoader(tpv_config, parent=loader)
        loader = current_loader
    return EntityToDestinationMapper(loader)


def setup_destination_mapper(app, referrer, tpv_configs: JOB_YAML_CONFIG_TYPE):
    mapper = load_destination_mapper(tpv_configs)

    for tpv_config in tpv_configs:
        if isinstance(tpv_config, str) and os.path.isfile(tpv_config):
            # adjust for tempfile handling on Darwin
            tpv_config_real_path = os.path.realpath(tpv_config)
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
                # watchdog on darwin notifies only once per file, so reload all mappers that refer to this file
                for referrer, config_files in REFERRERS_BY_CONFIG_FILE[
                    tpv_config_real_path
                ].items():
                    ACTIVE_DESTINATION_MAPPERS[referrer] = load_destination_mapper(
                        config_files, reload=True
                    )

            WATCHERS_BY_CONFIG_FILE[tpv_config_real_path] = watcher
            REFERRERS_BY_CONFIG_FILE[tpv_config_real_path][referrer] = tpv_configs
            watcher.watch_file(tpv_config_real_path, callback=reload_destination_mapper)
            watcher.start()

    return mapper


def lock_and_load_mapper(app, referrer, tpv_config):
    destination_mapper = ACTIVE_DESTINATION_MAPPERS.get(referrer)
    if not destination_mapper:
        # Try again with a lock
        # We need a lock to avoid thundering herd problems and to serialize access to WATCHERS_BY_CONFIG_FILE
        with DESTINATION_MAPPER_LOCK:
            destination_mapper = ACTIVE_DESTINATION_MAPPERS.get(referrer)
            # still null with the lock - must be the first time
            if not destination_mapper:
                destination_mapper = setup_destination_mapper(app, referrer, tpv_config)
                ACTIVE_DESTINATION_MAPPERS[referrer] = destination_mapper
    return destination_mapper


def map_tool_to_destination(
    app,
    job,
    tool,
    user,
    # the destination referring to the TPV dynamic destination, usually named "tpv_dispatcher"
    referrer="tpv_dispatcher",
    tpv_config_files: JOB_YAML_CONFIG_TYPE = None,
    tpv_configs: JOB_YAML_CONFIG_TYPE = None,
    job_wrapper=None,
    resource_params=None,
    workflow_invocation_uuid=None,
):
    if tpv_configs and tpv_config_files:
        raise ValueError(
            "Only one of tpv_configs or tpv_config_files can be specified in execution environment."
        )
    if not tpv_config_files and not tpv_configs:
        raise ValueError(
            "One of tpv_configs or tpv_config_files must be specified in execution environment."
        )
    tpv_configs = tpv_configs or tpv_config_files
    destination_mapper = lock_and_load_mapper(app, referrer, tpv_configs)
    return destination_mapper.map_to_destination(
        app, tool, user, job, job_wrapper, resource_params, workflow_invocation_uuid
    )
