import os
import tempfile

import pytest

from tpv.rules import gateway

# Galaxy's IntegrationTestCase now mixes in UsesCeleryTasks, whose autouse
# fixtures require the celery pytest plugin and a celery_includes fixture.
pytest_plugins = ("celery.contrib.pytest",)


@pytest.fixture(scope="session")
def celery_includes():
    return ["galaxy.celery.tasks"]


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")
    # Give each xdist worker its own mypy cache to avoid concurrent cache corruption
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    if worker_id:
        os.environ["MYPY_CACHE_DIR"] = os.path.join(tempfile.gettempdir(), f".mypy_cache_{worker_id}")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


@pytest.fixture(autouse=True)
def shutdown_watchers():
    for watcher in gateway.WATCHERS_BY_CONFIG_FILE.values():
        watcher.shutdown()
    gateway.WATCHERS_BY_CONFIG_FILE.clear()
    gateway.ACTIVE_DESTINATION_MAPPERS.clear()
    gateway.REFERRERS_BY_CONFIG_FILE.clear()
