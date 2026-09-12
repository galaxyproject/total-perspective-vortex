"""Resource pools against a real Galaxy and a real Valkey.

Every other pool test runs against mock_galaxy, and the Valkey adapter is otherwise exercised
only against fakeredis. These run a live Galaxy with an in-process job handler and the
production ValkeyAllocationStore, so the Lua admit script runs for real, and the tests can read
the ledger directly and assert exact accounting rather than infer it from job states.

Each test reaches a distinct branch of the Lua decision (see mapping-pools.yml):
budget fit and deferral, the oversize slot, hard_max rejection before any write, reserve_pool
in both directions, a full pool vetoing the multi-key write, terminal reconciliation, PERSIST,
and per-user isolation.

Requires a Valkey on localhost:6379:  docker run -d --rm -p 6379:6379 valkey/valkey:8
The tests skip without one, except in CI (TPV_TEST_REQUIRE_VALKEY set), where they fail.
"""

import os
import shutil
import tempfile
import time

import pytest
import yaml
from galaxy.webapps.base import webapp
from galaxy_test.base.populators import DatasetPopulator
from galaxy_test.driver.integration_util import IntegrationTestCase

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures", "pools")
VALKEY_URL = "redis://localhost:6379/0"
KEY_PREFIX = "tpv-integration-test:pool"  # the fixture's value; each worker derives its own from it
BUDGET_CORES = 8  # default pool, must match mapping-pools.yml
JOB_CORES = 4  # pool_sleep, must match mapping-pools.yml
ACTIVE = {"queued", "running"}


def _valkey():
    import redis

    return redis.from_url(VALKEY_URL, decode_responses=True, socket_connect_timeout=1)


def _require_or_skip_valkey():
    try:
        _valkey().ping()
    except Exception as e:  # noqa: BLE001 - any failure to reach the server means "not available"
        if os.environ.get("TPV_TEST_REQUIRE_VALKEY"):
            raise AssertionError(f"TPV_TEST_REQUIRE_VALKEY is set but Valkey is unreachable: {e}") from e
        pytest.skip(
            f"Valkey not reachable at {VALKEY_URL} ({e}); start one with: docker run -d --rm -p 6379:6379 valkey/valkey:8"
        )


class TestResourcePoolIntegration(IntegrationTestCase):
    default_tool_conf = os.path.join(FIXTURES, "tool_conf_pools.xml")

    @classmethod
    def setUpClass(cls):
        _require_or_skip_valkey()  # before the (expensive) Galaxy start-up
        cls._isolate_valkey_keys_per_worker()
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        shutil.rmtree(cls.config_dir, ignore_errors=True)

    @classmethod
    def _isolate_valkey_keys_per_worker(cls):
        """Under xdist every worker runs its own Galaxy with its own database, so user and job
        ids collide across workers (each Galaxy's first user and first job are id 1). Sharing
        one Valkey between them would mix ledgers and let one worker's setUp flush another's
        mid-test. Give each worker its own key_prefix: copy the fixture config with the prefix
        patched, and a job conf pointing at the copy. The fixture file stays the readable
        source of truth; only that one field differs."""
        worker = os.environ.get("PYTEST_XDIST_WORKER", "main")
        cls.key_prefix = f"{KEY_PREFIX}:{worker}"
        cls.config_dir = tempfile.mkdtemp(prefix="tpv-pools-")

        with open(os.path.join(FIXTURES, "mapping-pools.yml")) as f:
            mapping = yaml.safe_load(f)
        mapping["global"]["resource_pool_store"]["key_prefix"] = cls.key_prefix
        mapping_path = os.path.join(cls.config_dir, "mapping-pools.yml")
        with open(mapping_path, "w") as f:
            yaml.safe_dump(mapping, f)

        with open(os.path.join(FIXTURES, "job_conf_pools.yml")) as f:
            job_conf = yaml.safe_load(f)
        job_conf["execution"]["environments"]["tpv_dispatcher"]["tpv_config_files"] = [mapping_path]
        with open(os.path.join(cls.config_dir, "job_conf_pools.yml"), "w") as f:
            yaml.safe_dump(job_conf, f)

    @classmethod
    def handle_galaxy_config_kwds(cls, config):
        config["config_dir"] = cls.config_dir
        config["job_config_file"] = "job_conf_pools.yml"
        # Galaxy is a wheel in CI, not a checkout, so the sample configs it would otherwise
        # default to (lib/galaxy/config/sample/...) do not exist; point it at empty ones.
        config["tool_data_path"] = FIXTURES
        config["tool_data_table_config_path"] = os.path.join(FIXTURES, "tool_data_tables.xml.sample")
        config["data_manager_config_file"] = os.path.join(FIXTURES, "data_manager_conf.xml.sample")
        config["template_path"] = os.path.abspath(os.path.join(os.path.dirname(webapp.__file__), "templates"))

    def setUp(self):
        super().setUp()
        self.dataset_populator = DatasetPopulator(self.galaxy_interactor)
        self.valkey = _valkey()
        for key in self.valkey.scan_iter(f"{self.key_prefix}:*"):  # isolate tests from each other
            self.valkey.delete(key)

    # -- helpers -------------------------------------------------------------------------------

    def _submit(self, history_id, seconds, tool_id="pool_sleep"):
        response = self.dataset_populator.run_tool(tool_id, {"seconds": seconds}, history_id)
        return response["jobs"][0]["id"], response["outputs"][0]["id"]

    def _decode(self, encoded_id):
        return self._test_driver.app.security.decode_id(encoded_id)

    def _state(self, job_id):
        return self.dataset_populator.get_job_details(job_id).json()["state"]

    def _wait_for_state(self, job_id, states, timeout=60):
        deadline = time.time() + timeout
        state = None
        while time.time() < deadline:
            state = self._state(job_id)
            if state in states:
                return state
            time.sleep(0.5)
        raise AssertionError(f"job {job_id} did not reach {states} within {timeout}s (last state: {state})")

    def _assert_deferred(self, job_id, seconds=4):
        """A deferred job is left in 'new' by the handler: never queued, never started."""
        for _ in range(seconds * 2):
            time.sleep(0.5)
            assert self._state(job_id) == "new", f"job {job_id} was admitted while its pool was full"

    def _wait_all_ok(self, *job_ids, timeout=60):
        for job_id in job_ids:
            self.dataset_populator.wait_for_job(job_id, assert_ok=True, timeout=timeout)

    def _current_user_id(self):
        return self._decode(self.dataset_populator._get("users/current").json()["id"])

    def _ledger(self, user_id, pool="default"):
        """{job_id: {"cores", "mem", "gpus", "kind"}} straight from Valkey."""
        raw = self.valkey.hgetall(f"{self.key_prefix}:{pool}:user:{{{user_id}}}")
        ledger = {}
        for job_id, value in raw.items():
            cores, mem, gpus, kind = value.split("|")
            ledger[int(job_id)] = {"cores": float(cores), "mem": float(mem), "gpus": float(gpus), "kind": kind}
        return ledger

    # -- budget --------------------------------------------------------------------------------

    @pytest.mark.slow
    def test_budget_admits_exactly_two_at_a_time_across_five_jobs(self):
        user_id = self._current_user_id()
        with self.dataset_populator.test_history() as history_id:
            jobs = [self._submit(history_id, seconds=6)[0] for _ in range(5)]

            # Sample job states and the ledger together until every job is done: never more than
            # two active, never more than 8 cores recorded -- and at some point both slots in
            # use, or the pool is over-throttling.
            saw_full_pool = False
            deadline = time.time() + 120
            while time.time() < deadline:
                states = [self._state(j) for j in jobs]
                ledger = self._ledger(user_id)
                active = sum(s in ACTIVE for s in states)
                recorded = sum(e["cores"] for e in ledger.values())
                assert active <= BUDGET_CORES // JOB_CORES, f"{active} jobs active at once; states={states}"
                assert recorded <= BUDGET_CORES, f"ledger over budget: {ledger}"
                saw_full_pool = saw_full_pool or (active == 2 and recorded == BUDGET_CORES)
                if all(s == "ok" for s in states):
                    break
                assert not any(s in {"error", "failed"} for s in states), states
                time.sleep(0.5)
            else:
                raise AssertionError(f"jobs did not all finish: {[self._state(j) for j in jobs]}")
            assert saw_full_pool, "never observed both slots in use: the pool is over-throttling"

            # Finished jobs stay recorded until the next admission for this user reconciles them
            # against the job table. That admission must drop all five and record only the new
            # job -- and the key must have survived to be reconciled (no TTL).
            last, _ = self._submit(history_id, seconds=1)
            self._wait_all_ok(last)
            assert set(self._ledger(user_id)) == {self._decode(last)}

    @pytest.mark.slow
    def test_pools_are_per_user(self):
        with self.dataset_populator.test_history() as history_a:
            a_jobs = [self._submit(history_a, seconds=12)[0] for _ in range(2)]
            for j in a_jobs:
                self._wait_for_state(j, {"running"})
            a_blocked, _ = self._submit(history_a, seconds=1)  # A's pool is full

            # User B's identical job is admitted immediately: A's usage is not B's.
            with self._different_user("pool-user-b@vortex.org"):
                populator_b = DatasetPopulator(self.galaxy_interactor)
                with populator_b.test_history() as history_b:
                    b_job = populator_b.run_tool("pool_sleep", {"seconds": 1}, history_b)["jobs"][0]["id"]
                    populator_b.wait_for_job(b_job, assert_ok=True, timeout=30)

            assert self._state(a_blocked) == "new", "user A's job ran while A's pool was full"
            assert len(list(self.valkey.scan_iter(f"{self.key_prefix}:default:user:*"))) == 2, "one ledger per user"
            self._wait_all_ok(*a_jobs, a_blocked)

    # -- oversize ------------------------------------------------------------------------------

    @pytest.mark.slow
    def test_oversize_slot_admits_one_over_budget_job_then_defers_the_next(self):
        user_id = self._current_user_id()
        with self.dataset_populator.test_history() as history_id:
            first, _ = self._submit(history_id, seconds=10, tool_id="pool_sleep_oversize")
            self._wait_for_state(first, {"running"})
            assert self._ledger(user_id)[self._decode(first)]["kind"] == "oversize"

            second, _ = self._submit(history_id, seconds=1, tool_id="pool_sleep_oversize")
            self._assert_deferred(second)  # max_concurrent: 1
            assert set(self._ledger(user_id)) == {self._decode(first)}, "a deferred job must not be recorded"

            self._wait_all_ok(first, second)

    @pytest.mark.slow
    def test_request_above_the_ceiling_is_rejected_before_any_write(self):
        with self.dataset_populator.test_history() as history_id:
            job_id, output_id = self._submit(history_id, seconds=1, tool_id="pool_sleep_huge")
            state = self._wait_for_state(job_id, {"error", "failed", "ok", "running"})
            assert state == "error", f"a request that can never fit must fail permanently, got {state}"
            # Galaxy's handler turns JobMappingException.failure_message into job_wrapper.fail(),
            # which records it on the output dataset -- that is where the user sees why.
            output = self.dataset_populator.get_history_dataset_details(
                history_id, dataset_id=output_id, assert_ok=False, wait=False
            )
            assert "can never be scheduled" in output["misc_info"], output
            assert self._ledger(self._current_user_id()) == {}, "rejection must happen before any write"

    # -- reserve_pool --------------------------------------------------------------------------

    @pytest.mark.slow
    def test_reserve_pool_defers_a_normal_job_while_an_oversize_job_runs(self):
        with self.dataset_populator.test_history() as history_id:
            big, _ = self._submit(history_id, seconds=8, tool_id="pool_sleep_exclusive_oversize")
            self._wait_for_state(big, {"running"})
            small, _ = self._submit(history_id, seconds=1, tool_id="pool_sleep_exclusive")
            self._assert_deferred(small)  # fits the budget, but the pool is reserved
            self._wait_all_ok(big, small)

    @pytest.mark.slow
    def test_reserve_pool_defers_an_oversize_job_while_a_normal_job_runs(self):
        with self.dataset_populator.test_history() as history_id:
            small, _ = self._submit(history_id, seconds=8, tool_id="pool_sleep_exclusive")
            self._wait_for_state(small, {"running"})
            big, _ = self._submit(history_id, seconds=1, tool_id="pool_sleep_exclusive_oversize")
            self._assert_deferred(big)  # the oversize slot is free, but the pool is not empty
            self._wait_all_ok(small, big)

    # -- multi-key atomicity -------------------------------------------------------------------

    @pytest.mark.slow
    def test_a_full_pool_vetoes_the_write_to_every_pool_in_the_batch(self):
        """A GPU job is governed by both 'default' and 'gpu'. With the gpu pool full and the
        default pool half empty, the job must be deferred AND leave no trace in 'default': the
        Lua script checks every key before writing to any, so one full pool vetoes them all."""
        user_id = self._current_user_id()
        with self.dataset_populator.test_history() as history_id:
            first, _ = self._submit(history_id, seconds=10, tool_id="pool_sleep_gpu")
            self._wait_for_state(first, {"running"})
            first_id = self._decode(first)
            assert set(self._ledger(user_id, "default")) == {first_id}
            assert set(self._ledger(user_id, "gpu")) == {first_id}
            assert self._ledger(user_id, "gpu")[first_id]["gpus"] == 1

            second, _ = self._submit(history_id, seconds=6, tool_id="pool_sleep_gpu")
            self._assert_deferred(second)
            # The decisive check: default had room for it (4 + 4 <= 8) and must still not hold it.
            assert set(self._ledger(user_id, "default")) == {
                first_id
            }, "partial write: one pool admitted a job another vetoed"
            assert set(self._ledger(user_id, "gpu")) == {first_id}

            # When the first finishes, the second is admitted to both pools in one write.
            self._wait_all_ok(first)
            self._wait_for_state(second, ACTIVE)
            second_id = self._decode(second)
            assert second_id in self._ledger(user_id, "default")
            assert second_id in self._ledger(user_id, "gpu")
            self._wait_all_ok(second)
