import os
import unittest

from galaxy.jobs.mapper import JobMappingException, JobNotReadyException

from tpv.commands.test import mock_galaxy
from tpv.core.loader import TPVConfigLoader
from tpv.core.mapper import EntityToDestinationMapper
from tpv.core.resource_pool import (
    NORMAL,
    OVERSIZE,
    Budget,
    ConfigBudgetProvider,
    InMemoryAllocationStore,
    ResourcePoolConfig,
    ResourceUsage,
    StoreUnavailable,
    ValkeyAllocationStore,
    terminal_job_ids,
)

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures/mapping-resource-pool.yml")
UNLIMITED = Budget()


def _seed(store, pool, user_id, job_id, usage, kind=NORMAL):
    """Force a ledger entry regardless of budget (used to simulate pre-existing jobs)."""
    assert store.admit(
        pool,
        user_id,
        job_id,
        usage,
        kind=kind,
        budget=UNLIMITED,
        max_oversize=10**9,
        reserve_pool=False,
        drop_job_ids=set(),
    )


class TestAllocationStore(unittest.TestCase):
    def setUp(self):
        self.store = InMemoryAllocationStore()
        self.budget = Budget(cores=32, mem=256, gpus=2)

    def test_admit_and_read(self):
        ok = self.store.admit(
            "default",
            1,
            100,
            ResourceUsage(8, 16, 0),
            kind=NORMAL,
            budget=self.budget,
            max_oversize=0,
            reserve_pool=False,
            drop_job_ids=set(),
        )
        self.assertTrue(ok)
        ledger = self.store.read("default", 1)
        self.assertEqual(ledger[100], (ResourceUsage(8, 16, 0), NORMAL))

    def test_over_budget_defers(self):
        _seed(self.store, "default", 1, 100, ResourceUsage(28, 16, 0))
        # 28 + 8 = 36 > 32 cores
        ok = self.store.admit(
            "default",
            1,
            101,
            ResourceUsage(8, 16, 0),
            kind=NORMAL,
            budget=self.budget,
            max_oversize=0,
            reserve_pool=False,
            drop_job_ids=set(),
        )
        self.assertFalse(ok)
        self.assertNotIn(101, self.store.read("default", 1))

    def test_pool_separation_by_name_and_user(self):
        _seed(self.store, "default", 1, 100, ResourceUsage(32, 0, 0))
        # Same user, different pool -> independent ledger.
        self.assertEqual(self.store.read("gpu", 1), {})
        # Different user -> independent ledger.
        self.assertEqual(self.store.read("default", 2), {})

    def test_oversize_count_limit(self):
        ok1 = self.store.admit(
            "default",
            1,
            100,
            ResourceUsage(64, 0, 0),
            kind=OVERSIZE,
            budget=self.budget,
            max_oversize=1,
            reserve_pool=False,
            drop_job_ids=set(),
        )
        self.assertTrue(ok1)
        ok2 = self.store.admit(
            "default",
            1,
            101,
            ResourceUsage(64, 0, 0),
            kind=OVERSIZE,
            budget=self.budget,
            max_oversize=1,
            reserve_pool=False,
            drop_job_ids=set(),
        )
        self.assertFalse(ok2)

    def test_reserve_pool_blocks_co_tenancy(self):
        _seed(self.store, "default", 1, 100, ResourceUsage(64, 0, 0), kind=OVERSIZE)
        # A normal job cannot run while an oversize job holds the reserved pool.
        ok = self.store.admit(
            "default",
            1,
            101,
            ResourceUsage(1, 0, 0),
            kind=NORMAL,
            budget=self.budget,
            max_oversize=1,
            reserve_pool=True,
            drop_job_ids=set(),
        )
        self.assertFalse(ok)

    def test_drop_job_ids_releases_then_admits(self):
        _seed(self.store, "default", 1, 100, ResourceUsage(30, 0, 0))
        # Without dropping, this would not fit (30 + 8 > 32). Dropping 100 frees the pool.
        ok = self.store.admit(
            "default",
            1,
            101,
            ResourceUsage(8, 0, 0),
            kind=NORMAL,
            budget=self.budget,
            max_oversize=0,
            reserve_pool=False,
            drop_job_ids={100},
        )
        self.assertTrue(ok)
        self.assertNotIn(100, self.store.read("default", 1))

    def test_readmitting_same_job_does_not_double_count(self):
        _seed(self.store, "default", 1, 100, ResourceUsage(30, 0, 0))
        # Re-admitting job 100 (e.g. resubmission) replaces its entry rather than stacking.
        ok = self.store.admit(
            "default",
            1,
            100,
            ResourceUsage(30, 0, 0),
            kind=NORMAL,
            budget=self.budget,
            max_oversize=0,
            reserve_pool=False,
            drop_job_ids=set(),
        )
        self.assertTrue(ok)
        self.assertEqual(len(self.store.read("default", 1)), 1)


class TestBudgetProvider(unittest.TestCase):
    def test_flat_per_pool_budget(self):
        config = ResourcePoolConfig.model_validate(
            {"pools": {"gpu": {"max_gpus": 2}, "cpu": {"max_cores": 16, "max_mem": 64}}}
        )
        provider = ConfigBudgetProvider(config)
        self.assertEqual(provider.budget_for(None, "gpu"), Budget(cores=None, mem=None, gpus=2))
        self.assertEqual(provider.budget_for(None, "cpu"), Budget(cores=16, mem=64, gpus=None))
        self.assertEqual(provider.budget_for(None, "missing"), Budget())


class TestTerminalJobIds(unittest.TestCase):
    def test_returns_only_terminal_jobs(self):
        app = mock_galaxy.App(create_model=True)
        sa_session = app.model.context
        user = app.model.User(username="trillian", email="trillian@vortex.org", password="x")
        sa_session.add(user)
        sa_session.flush()

        def make_job(state):
            job = app.model.Job()
            job.user = user
            job.tool_id = "t"
            job.state = state
            sa_session.add(job)
            sa_session.flush()
            return job.id

        running = make_job("running")
        done = make_job("ok")
        errored = make_job("error")

        self.assertEqual(terminal_job_ids(app.model.context, set()), set())
        self.assertEqual(
            terminal_job_ids(app.model.context, {running, done, errored}),
            {done, errored},
        )


class TestResourcePoolMapping(unittest.TestCase):
    def _mapper(self):
        return EntityToDestinationMapper(TPVConfigLoader.from_url_or_path(FIXTURE))

    @staticmethod
    def _job(job_id):
        job = mock_galaxy.Job()
        job.id = job_id
        return job

    def _app(self):
        return mock_galaxy.App(create_model=True)

    def test_under_budget_maps(self):
        mapper = self._mapper()
        user = mock_galaxy.User("arthur", "arthur@vortex.org", id=1)
        dest = mapper.map_to_destination(self._app(), mock_galaxy.Tool("default"), user, self._job(1))
        self.assertEqual(dest.id, "local")
        # The job was recorded against the pool.
        self.assertIn(1, mapper.resource_pools.store.read("default", 1))

    def test_over_budget_defers(self):
        mapper = self._mapper()
        user = mock_galaxy.User("arthur", "arthur@vortex.org", id=1)
        _seed(mapper.resource_pools.store, "default", 1, 900, ResourceUsage(30, 0, 0))
        with self.assertRaises(JobNotReadyException):
            mapper.map_to_destination(self._app(), mock_galaxy.Tool("default"), user, self._job(1))

    def test_per_user_isolation(self):
        mapper = self._mapper()
        arthur = mock_galaxy.User("arthur", "arthur@vortex.org", id=1)
        ford = mock_galaxy.User("ford", "ford@vortex.org", id=2)
        _seed(mapper.resource_pools.store, "default", arthur.id, 900, ResourceUsage(30, 0, 0))
        with self.assertRaises(JobNotReadyException):
            mapper.map_to_destination(self._app(), mock_galaxy.Tool("default"), arthur, self._job(1))
        # ford is unaffected by arthur's full pool.
        dest = mapper.map_to_destination(self._app(), mock_galaxy.Tool("default"), ford, self._job(2))
        self.assertEqual(dest.id, "local")

    def test_oversize_admitted_then_second_defers(self):
        mapper = self._mapper()
        user = mock_galaxy.User("zaphod", "zaphod@vortex.org", id=3)
        # 64 cores > 32 budget -> oversize, allowed up to max_concurrent=1.
        dest = mapper.map_to_destination(self._app(), mock_galaxy.Tool("bigtool"), user, self._job(10))
        self.assertEqual(dest.id, "local")
        # A second oversize job exceeds max_concurrent=1 -> deferred.
        with self.assertRaises(JobNotReadyException):
            mapper.map_to_destination(self._app(), mock_galaxy.Tool("bigtool"), user, self._job(11))

    def test_oversize_beyond_hard_max_fails(self):
        mapper = self._mapper()
        user = mock_galaxy.User("zaphod", "zaphod@vortex.org", id=3)
        with self.assertRaisesRegex(JobMappingException, "can never be scheduled"):
            mapper.map_to_destination(self._app(), mock_galaxy.Tool("hugetool"), user, self._job(12))

    def test_udt_pool_without_oversize_fails(self):
        mapper = self._mapper()
        user = mock_galaxy.User("zaphod", "zaphod@vortex.org", id=3)
        with self.assertRaisesRegex(JobMappingException, "can never be scheduled"):
            mapper.map_to_destination(self._app(), mock_galaxy.Tool("udt_tool"), user, self._job(13))

    def test_reconciliation_releases_terminal_jobs(self):
        mapper = self._mapper()
        app = self._app()
        sa_session = app.model.context
        db_user = app.model.User(username="marvin", email="marvin@vortex.org", password="x")
        sa_session.add(db_user)
        sa_session.flush()
        finished = app.model.Job()
        finished.user = db_user
        finished.tool_id = "default"
        finished.state = "ok"
        sa_session.add(finished)
        sa_session.flush()

        user = mock_galaxy.User("marvin", "marvin@vortex.org", id=db_user.id)
        # The finished job's allocation lingers in the ledger and would otherwise fill the pool.
        _seed(mapper.resource_pools.store, "default", user.id, finished.id, ResourceUsage(30, 0, 0))
        # Mapping reconciles the terminal job out, freeing the pool for the new job.
        dest = mapper.map_to_destination(app, mock_galaxy.Tool("default"), user, self._job(20))
        self.assertEqual(dest.id, "local")
        ledger = mapper.resource_pools.store.read("default", user.id)
        self.assertNotIn(finished.id, ledger)
        self.assertIn(20, ledger)

    def test_oversize_beyond_hard_max_mem_fails(self):
        # 600GB mem > hard_max_mem of 512 (cores fit) -> the memory ceiling is enforced too.
        mapper = self._mapper()
        user = mock_galaxy.User("zaphod", "zaphod@vortex.org", id=3)
        with self.assertRaisesRegex(JobMappingException, "can never be scheduled"):
            mapper.map_to_destination(self._app(), mock_galaxy.Tool("bigmem_tool"), user, self._job(14))

    def test_gpu_pool_admits_and_defers(self):
        mapper = self._mapper()
        user = mock_galaxy.User("zaphod", "zaphod@vortex.org", id=3)
        # A GPU job within the 2-GPU budget maps and is recorded against the gpu pool.
        dest = mapper.map_to_destination(self._app(), mock_galaxy.Tool("gpu_tool"), user, self._job(15))
        self.assertEqual(dest.id, "local")
        self.assertEqual(mapper.resource_pools.store.read("gpu", user.id)[15][0].gpus, 1)

    def test_gpu_pool_defers_when_full(self):
        mapper = self._mapper()
        user = mock_galaxy.User("zaphod", "zaphod@vortex.org", id=3)
        # The user already holds both GPUs; a further GPU job is deferred even though its
        # CPU/memory request would fit the default pool.
        _seed(mapper.resource_pools.store, "gpu", user.id, 800, ResourceUsage(0, 0, 2))
        with self.assertRaises(JobNotReadyException):
            mapper.map_to_destination(self._app(), mock_galaxy.Tool("gpu_tool"), user, self._job(16))


def _build_valkey_store():
    """A ValkeyAllocationStore backed by fakeredis, or None when fakeredis+Lua is unavailable.

    Lets the parity test run the *real* Valkey adapter (Lua admit script, ``|`` serialisation,
    URL handling) without a live server, falling back to skip when the dependency is absent.
    """
    try:
        import fakeredis
    except ImportError:
        return None
    try:
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        return ValkeyAllocationStore(client=client)
    except Exception:
        return None


def _op(job_id, req, *, kind=NORMAL, budget=Budget(32, 256, 2), max_oversize=0, reserve_pool=False, drop=()):
    return dict(
        pool="p",
        user_id=1,
        job_id=job_id,
        req=req,
        kind=kind,
        budget=budget,
        max_oversize=max_oversize,
        reserve_pool=reserve_pool,
        drop_job_ids=set(drop),
    )


# Each scenario is a sequence of admit() calls applied to a fresh store. The parity test asserts
# the in-memory and Valkey/Lua implementations return identical decisions and leave identical
# ledgers -- pinning the duplicated admission logic (Python _decide vs the _ADMIT_LUA script) to
# one contract, and covering the cores/mem/gpus/unlimited dimensions.
PARITY_SCENARIOS = {
    "cores_over_budget": [_op(1, ResourceUsage(28, 0, 0)), _op(2, ResourceUsage(8, 0, 0))],
    "mem_over_budget": [
        _op(1, ResourceUsage(1, 10, 0), budget=Budget(100, 16, 2)),
        _op(2, ResourceUsage(1, 10, 0), budget=Budget(100, 16, 2)),
    ],
    "gpu_over_budget": [_op(1, ResourceUsage(1, 0, 2)), _op(2, ResourceUsage(1, 0, 1))],
    "unlimited_dimensions": [
        _op(1, ResourceUsage(4, 9999, 9999), budget=Budget(4, None, None)),
        _op(2, ResourceUsage(1, 0, 0), budget=Budget(4, None, None)),
    ],
    "oversize_count_limit": [
        _op(1, ResourceUsage(64, 0, 0), kind=OVERSIZE, max_oversize=1),
        _op(2, ResourceUsage(64, 0, 0), kind=OVERSIZE, max_oversize=1),
    ],
    "reserve_pool": [
        _op(1, ResourceUsage(64, 0, 0), kind=OVERSIZE, max_oversize=1, reserve_pool=True),
        _op(2, ResourceUsage(1, 0, 0), kind=NORMAL, max_oversize=1, reserve_pool=True),
    ],
    "drop_releases": [_op(1, ResourceUsage(30, 0, 0)), _op(2, ResourceUsage(8, 0, 0), drop=(1,))],
}


class TestStoreParity(unittest.TestCase):
    def test_valkey_matches_in_memory(self):
        if _build_valkey_store() is None:
            self.skipTest("fakeredis with Lua support not available")
        for name, ops in PARITY_SCENARIOS.items():
            with self.subTest(scenario=name):
                in_memory = InMemoryAllocationStore()
                valkey = _build_valkey_store()
                mem_results = [in_memory.admit(**op) for op in ops]
                vk_results = [valkey.admit(**op) for op in ops]
                self.assertEqual(mem_results, vk_results, f"admit decisions differ for {name}")
                self.assertEqual(
                    in_memory.read("p", 1),
                    valkey.read("p", 1),
                    f"final ledgers differ for {name}",
                )


if __name__ == "__main__":
    unittest.main()
