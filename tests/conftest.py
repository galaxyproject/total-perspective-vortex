import pytest

from tpv.rules import gateway


def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


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
