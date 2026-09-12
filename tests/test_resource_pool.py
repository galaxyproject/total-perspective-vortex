import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import patch

from galaxy.jobs.mapper import JobMappingException, JobNotReadyException

from tpv.commands.test import mock_galaxy
from tpv.core.entities import PoolEntity, Rule, SchedulingTags, Tool
from tpv.core.loader import TPVConfigLoader
from tpv.core.mapper import EntityToDestinationMapper
from tpv.core.resource_pool import (
    NORMAL,
    OVERSIZE,
    AllocationStore,
    Budget,
    InMemoryAllocationStore,
    PoolAdmission,
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


class TestStoreConfig(unittest.TestCase):
    def test_builds_store_from_class_and_options(self):
        from tpv.core.resource_pool import StoreConfig

        config = StoreConfig.model_validate(
            {"class": "tpv.core.resource_pool.InMemoryAllocationStore", "key_prefix": "custom"}
        )
        store = config.build_store()
        self.assertIsInstance(store, InMemoryAllocationStore)
        # The extra option was passed through to the store constructor.
        self.assertEqual(store.key_prefix, "custom")

    def test_default_store_class_is_valkey(self):
        from tpv.core.resource_pool import StoreConfig

        self.assertEqual(StoreConfig().store_class, "tpv.core.resource_pool.ValkeyAllocationStore")


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


class TestPoolInheritance(unittest.TestCase):
    def test_omitted_and_partial_policy_inherit_across_multiple_levels(self):
        pools = TPVConfigLoader(
            {
                "pools": {
                    "parent": {
                        "fail_open": True,
                        "max_concurrent_cores": 32,
                        "oversize": {"max_concurrent": 1, "hard_max_cores": 128, "reserve_pool": True},
                    },
                    "child": {"inherits": "parent"},
                    "grandchild": {"inherits": "child", "oversize": {"max_concurrent": 2}},
                }
            }
        ).config.pools
        self.assertEqual(pools["child"].oversize, pools["parent"].oversize)
        self.assertTrue(pools["child"].fail_open)
        self.assertEqual(pools["grandchild"].oversize.max_concurrent, 2)
        self.assertEqual(pools["grandchild"].oversize.hard_max_cores, 128)
        self.assertTrue(pools["grandchild"].oversize.reserve_pool)
        self.assertEqual(pools["grandchild"].max_concurrent_cores, 32)
        self.assertEqual(pools["parent"].oversize.max_concurrent, 1)

    def test_explicit_false_zero_and_null_override_parent_policy(self):
        pools = TPVConfigLoader(
            {
                "pools": {
                    "parent": {
                        "fail_open": True,
                        "oversize": {"max_concurrent": 1, "hard_max_cores": 128, "reserve_pool": True},
                    },
                    "child": {
                        "inherits": "parent",
                        "fail_open": False,
                        "oversize": {"max_concurrent": 0, "hard_max_cores": None, "reserve_pool": False},
                    },
                }
            }
        ).config.pools
        self.assertFalse(pools["child"].fail_open)
        self.assertEqual(pools["child"].oversize.max_concurrent, 0)
        self.assertIsNone(pools["child"].oversize.hard_max_cores)
        self.assertFalse(pools["child"].oversize.reserve_pool)

    def test_overlay_config_preserves_unspecified_policy(self):
        parent = TPVConfigLoader(
            {"pools": {"p": {"fail_open": True, "oversize": {"max_concurrent": 1, "hard_max_mem": 512}}}}
        )
        child = TPVConfigLoader({"pools": {"p": {"oversize": {"hard_max_cores": 128}}}}, parent=parent)
        pool = child.config.pools["p"]
        self.assertTrue(pool.fail_open)
        self.assertEqual(pool.oversize.max_concurrent, 1)
        self.assertEqual(pool.oversize.hard_max_mem, 512)
        self.assertEqual(pool.oversize.hard_max_cores, 128)


class TestPoolMembership(unittest.TestCase):
    def test_untagged_pool_covers_required_routing_tags(self):
        pool = PoolEntity()
        self.assertTrue(pool.matches(Tool(scheduling={"require": ["gpu", "pulsar"]})))

    def test_pool_selects_positive_job_tags_only(self):
        pool = PoolEntity(scheduling={"require": ["gpu"], "reject": ["udt"]})
        for tag_type in ("require", "prefer", "accept"):
            with self.subTest(tag_type=tag_type):
                self.assertTrue(pool.matches(Tool(scheduling={tag_type: ["gpu"], "reject": ["udt"]})))
                self.assertFalse(pool.matches(Tool(scheduling={tag_type: ["gpu", "udt"]})))
        self.assertFalse(pool.matches(Tool(scheduling={"reject": ["gpu"]})))
        self.assertFalse(pool.matches(Tool()))


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

    def test_abstract_pool_is_only_an_inheritance_template(self):
        loader = TPVConfigLoader.from_url_or_path(FIXTURE)
        loader.config.pools["template"] = PoolEntity(
            id="template",
            abstract=True,
            max_concurrent_cores=1,
            scheduling={"require": ["gpu"]},
            evaluator=loader,
        )
        loader.config.pools["gpu"] = PoolEntity(
            id="gpu",
            inherits="template",
            max_concurrent_cores=8,
            evaluator=loader,
        )
        loader.process_entities(loader.config)
        mapper = EntityToDestinationMapper(loader)
        user = mock_galaxy.User("arthur", "arthur@vortex.org", id=1)
        dest = mapper.map_to_destination(self._app(), mock_galaxy.Tool("gpu_tool"), user, self._job(1))
        self.assertEqual(dest.id, "local")
        self.assertEqual(mapper.resource_pools.store.read("template", user.id), {})
        self.assertIn(1, mapper.resource_pools.store.read("gpu", user.id))

    def test_under_budget_maps(self):
        mapper = self._mapper()
        user = mock_galaxy.User("arthur", "arthur@vortex.org", id=1)
        dest = mapper.map_to_destination(self._app(), mock_galaxy.Tool("default"), user, self._job(1))
        self.assertEqual(dest.id, "local")
        # The job was recorded against the pool.
        self.assertIn(1, mapper.resource_pools.store.read("default", 1))

    def test_required_routing_tag_does_not_bypass_default_budget(self):
        mapper = self._mapper()
        mapper.config.tools["default"].tpv_tags = SchedulingTags(require=["gpu"])
        mapper.destinations["local"].tpv_dest_tags = SchedulingTags(accept=["gpu"])
        user = mock_galaxy.User("arthur", "arthur@vortex.org", id=1)
        _seed(mapper.resource_pools.store, "default", user.id, 900, ResourceUsage(32, 0, 0))
        with self.assertRaises(JobNotReadyException):
            mapper.map_to_destination(self._app(), mock_galaxy.Tool("default"), user, self._job(1))

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

    def test_job_with_require_tags_is_still_governed_by_an_untagged_pool(self):
        # A pool selects which jobs it governs; it does not negotiate capabilities with them.
        # A job's own require tags describe what it needs from a *destination*, and must not
        # exempt it from an untagged pool -- otherwise enforcement is silently bypassed.
        mapper = self._mapper()
        user = mock_galaxy.User("arthur", "arthur@vortex.org", id=1)
        mapper.map_to_destination(self._app(), mock_galaxy.Tool("tagged_tool"), user, self._job(50))
        self.assertIn(50, mapper.resource_pools.store.read("default", 1))

    def test_pool_reject_tag_still_excludes_a_job(self):
        # The 'default' pool rejects 'udt', so a udt job is governed only by the udt pool.
        mapper = self._mapper()
        user = mock_galaxy.User("zaphod", "zaphod@vortex.org", id=3)
        with self.assertRaises(JobMappingException):
            mapper.map_to_destination(self._app(), mock_galaxy.Tool("udt_tool"), user, self._job(51))
        self.assertEqual(mapper.resource_pools.store.read("default", 3), {})

    def test_prefer_and_accept_are_rejected_on_pools(self):
        # Pools select jobs with require/reject only. prefer/accept have no meaning (there is no
        # ranking among pools), so they must fail loudly at load rather than silently do nothing.
        from tpv.core.entities import PoolEntity

        for kind in ("prefer", "accept"):
            with self.subTest(kind=kind):
                with self.assertRaisesRegex(ValueError, f"scheduling.{kind}"):
                    PoolEntity.model_validate({"scheduling": {kind: ["gpu"]}})

    def test_gpu_deferrals_leave_cpu_budget_available(self):
        mapper = self._mapper()
        app = self._app()
        user = mock_galaxy.User("arthur", "arthur@vortex.org", id=1)
        _seed(mapper.resource_pools.store, "gpu", user.id, 900, ResourceUsage(0, 0, 2))
        for jid in range(1, 9):
            with self.assertRaises(JobNotReadyException):
                mapper.map_to_destination(app, mock_galaxy.Tool("gpu_tool"), user, self._job(jid))
        self.assertEqual(mapper.resource_pools.store.read("default", user.id), {})
        dest = mapper.map_to_destination(app, mock_galaxy.Tool("default"), user, self._job(10))
        self.assertEqual(dest.id, "local")

    def test_permanent_rejection_leaves_no_partial_allocations(self):
        mapper = self._mapper()
        mapper.pools["z_last"] = PoolEntity(max_concurrent_cores=1)
        user = mock_galaxy.User("arthur", "arthur@vortex.org", id=1)
        with self.assertRaisesRegex(JobMappingException, "can never be scheduled"):
            mapper.map_to_destination(self._app(), mock_galaxy.Tool("default"), user, self._job(1))
        self.assertEqual(mapper.resource_pools.store.read("default", user.id), {})

    def test_destination_rejection_leaves_no_allocations(self):
        for exception in ("TryNextDestinationOrWait", "TryNextDestinationOrFail"):
            with self.subTest(exception=exception):
                mapper = self._mapper()
                mapper.destinations["local"].rules = {
                    "reject": Rule(
                        **{
                            "if": True,
                            "execute": f"from tpv.core.entities import {exception}\nraise {exception}()\nNone",
                            "evaluator": mapper.loader,
                        }
                    )
                }
                user = mock_galaxy.User("arthur", "arthur@vortex.org", id=1)
                with self.assertRaises((JobNotReadyException, JobMappingException)):
                    mapper.map_to_destination(self._app(), mock_galaxy.Tool("default"), user, self._job(1))
                self.assertEqual(mapper.resource_pools.store.read("default", user.id), {})

    def test_destination_clamping_does_not_change_billed_request(self):
        mapper = self._mapper()
        mapper.destinations["local"].max_cores = 1
        user = mock_galaxy.User("arthur", "arthur@vortex.org", id=1)
        dest = mapper.map_to_destination(self._app(), mock_galaxy.Tool("default"), user, self._job(1))
        self.assertEqual(dest.params["native_spec"], "--cores 1 --mem 2")
        self.assertEqual(mapper.resource_pools.store.read("default", user.id)[1][0], ResourceUsage(4, 8, 0))

    def test_user_budget_override_wins_over_pool_default(self):
        # trillian's user entity raises max_concurrent_cores to 128; combine resolves it over the
        # default pool's 32, so two 64-core bigtool jobs are admitted as *normal* (not oversize).
        mapper = self._mapper()
        trillian = mock_galaxy.User("trillian", "trillian@vortex.org", id=4)
        d1 = mapper.map_to_destination(self._app(), mock_galaxy.Tool("bigtool"), trillian, self._job(30))
        d2 = mapper.map_to_destination(self._app(), mock_galaxy.Tool("bigtool"), trillian, self._job(31))
        self.assertEqual((d1.id, d2.id), ("local", "local"))
        ledger = mapper.resource_pools.store.read("default", trillian.id)
        self.assertEqual({jid: kind for jid, (_, kind) in ledger.items()}, {30: NORMAL, 31: NORMAL})


class _RaisingStore(AllocationStore):
    """A store whose backend is always unreachable, to exercise the fail-closed path."""

    def read(self, pool, user_id):
        raise StoreUnavailable("backend down")

    def admit_many(self, *args, **kwargs):
        raise StoreUnavailable("backend down")


class TestResourcePoolBranches(unittest.TestCase):
    """Covers the enforcement branches that carry the safety story: fail-closed, fail_open,
    anonymous no-op and pools-disabled no-op."""

    def _mapper(self):
        return EntityToDestinationMapper(TPVConfigLoader.from_url_or_path(FIXTURE))

    @staticmethod
    def _job(job_id):
        job = mock_galaxy.Job()
        job.id = job_id
        return job

    def _app(self):
        return mock_galaxy.App(create_model=True)

    def test_store_unavailable_defers_fail_closed(self):
        mapper = self._mapper()
        mapper.resource_pools.store = _RaisingStore()
        user = mock_galaxy.User("arthur", "arthur@vortex.org", id=1)
        with self.assertRaises(JobNotReadyException):
            mapper.map_to_destination(self._app(), mock_galaxy.Tool("default"), user, self._job(40))

    def test_fail_open_pool_admits_when_store_unavailable(self):
        mapper = self._mapper()
        mapper.resource_pools.store = _RaisingStore()
        # A non-security pool marked fail_open lets jobs through during a store outage.
        mapper.pools["default"].fail_open = True
        user = mock_galaxy.User("arthur", "arthur@vortex.org", id=1)
        dest = mapper.map_to_destination(self._app(), mock_galaxy.Tool("default"), user, self._job(41))
        self.assertEqual(dest.id, "local")

    def test_batch_write_failure_requires_every_pool_to_fail_open(self):
        for strict in (True, False):
            with self.subTest(strict=strict):
                mapper = self._mapper()
                mapper.pools["default"].fail_open = True
                mapper.pools["gpu"].fail_open = not strict
                user = mock_galaxy.User("arthur", "arthur@vortex.org", id=1)
                with patch.object(mapper.resource_pools.store, "admit_many", side_effect=StoreUnavailable("down")):
                    if strict:
                        with self.assertRaises(JobNotReadyException):
                            mapper.map_to_destination(self._app(), mock_galaxy.Tool("gpu_tool"), user, self._job(1))
                    else:
                        dest = mapper.map_to_destination(self._app(), mock_galaxy.Tool("gpu_tool"), user, self._job(1))
                        self.assertEqual(dest.id, "local")
                self.assertEqual(mapper.resource_pools.store.read("default", user.id), {})

    def test_anonymous_user_is_not_governed(self):
        mapper = self._mapper()
        # No user -> per-user pools do not apply; the job maps and nothing is recorded.
        dest = mapper.map_to_destination(self._app(), mock_galaxy.Tool("default"), None, self._job(42))
        self.assertEqual(dest.id, "local")

    def test_pools_disabled_is_noop(self):
        # A config without a resource_pool_store / pools leaves enforcement entirely off.
        basic = os.path.join(os.path.dirname(__file__), "fixtures/mapping-basic.yml")
        mapper = EntityToDestinationMapper(TPVConfigLoader.from_url_or_path(basic))
        self.assertIsNone(mapper.resource_pools)
        user = mock_galaxy.User("arthur", "arthur@vortex.org", id=1)
        dest = mapper.map_to_destination(self._app(), mock_galaxy.Tool("bwa"), user, self._job(43))
        self.assertIsNotNone(dest.id)

    def test_pools_without_store_is_rejected(self):
        # Declaring pool policy but omitting the store wiring must fail loudly, not silently
        # disable enforcement (fail-open by omission).
        loader = TPVConfigLoader.from_url_or_path(FIXTURE)
        loader.config.global_config.resource_pool_store = None
        with self.assertRaisesRegex(ValueError, "resource_pool_store"):
            EntityToDestinationMapper(loader)


def _build_valkey_store():
    """A ValkeyAllocationStore backed by fakeredis, or None when fakeredis+Lua is unavailable.

    Lets the parity test run the *real* Valkey adapter (Lua admit script, ``|`` serialisation,
    URL handling) without a live server. The contract test fails if the dependency is absent.
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


class TestLedgerLifetime(unittest.TestCase):
    def test_idle_ledger_keeps_running_allocation(self):
        store = _build_valkey_store()
        self.assertIsNotNone(store)
        _seed(store, "p", 1, 1, ResourceUsage(32, 0, 0))
        import time

        with patch("time.time", return_value=time.time() + 7200):
            self.assertIn(1, store.read("p", 1))
            self.assertFalse(store.admit(**_op(2, ResourceUsage(1, 0, 0))))

    def test_admission_removes_preexisting_key_expiry(self):
        store = _build_valkey_store()
        self.assertIsNotNone(store)
        _seed(store, "p", 1, 1, ResourceUsage(32, 0, 0))
        key = store._key("p", 1)
        store.client.expire(key, 3600)
        self.assertFalse(store.admit(**_op(2, ResourceUsage(1, 0, 0))))
        self.assertEqual(store.client.ttl(key), -1)

    def test_expiring_ledgers_are_rejected(self):
        for ttl in (-1, 3600):
            with self.subTest(ttl=ttl), self.assertRaisesRegex(ValueError, "cannot expire"):
                ValkeyAllocationStore(ttl=ttl)


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


def _admission(pool, cores=4, *, drop=()):
    return PoolAdmission(pool, ResourceUsage(cores, 0, 0), NORMAL, Budget(4, None, None), 0, False, set(drop))


class TestAtomicPoolAdmission(unittest.TestCase):
    def test_rejection_in_either_pool_is_atomic(self):
        for build in (InMemoryAllocationStore, _build_valkey_store):
            for full_pool in ("a", "b"):
                with self.subTest(store=build.__name__, full_pool=full_pool):
                    store = build()
                    _seed(store, full_pool, 1, 99, ResourceUsage(4, 0, 0))
                    self.assertFalse(store.admit_many(1, 1, [_admission("a"), _admission("b")]))
                    for pool in ("a", "b"):
                        self.assertNotIn(1, store.read(pool, 1))
                    self.assertIn(99, store.read(full_pool, 1))

    def test_rejected_retry_clears_old_entries_in_all_pools(self):
        for build in (InMemoryAllocationStore, _build_valkey_store):
            with self.subTest(store=build.__name__):
                store = build()
                for pool in ("a", "b"):
                    _seed(store, pool, 1, 1, ResourceUsage(1, 0, 0))
                _seed(store, "a", 1, 99, ResourceUsage(4, 0, 0))
                self.assertFalse(store.admit_many(1, 1, [_admission("a"), _admission("b")]))
                self.assertEqual(set(store.read("a", 1)), {99})
                self.assertEqual(store.read("b", 1), {})
                self.assertTrue(store.admit_many(1, 1, [_admission("a", drop={99}), _admission("b")]))
                self.assertTrue(store.admit_many(1, 1, [_admission("a"), _admission("b")]))
                for pool in ("a", "b"):
                    self.assertEqual(store.read(pool, 1), {1: (ResourceUsage(4, 0, 0), NORMAL)})

    def test_concurrent_jobs_cannot_oversubscribe_or_split_pools(self):
        for build in (InMemoryAllocationStore, _build_valkey_store):
            with self.subTest(store=build.__name__):
                store = build()
                # Warm up Lua loading before the racing admission calls.
                _seed(store, "warmup", 1, 99, ResourceUsage())
                barrier = Barrier(8)

                def admit(jid):
                    barrier.wait(timeout=10)
                    return store.admit_many(1, jid, [_admission("a"), _admission("b")])

                with ThreadPoolExecutor(max_workers=8) as executor:
                    results = list(executor.map(admit, range(1, 9)))
                self.assertEqual(sum(results), 1)
                self.assertEqual(store.read("a", 1), store.read("b", 1))
                self.assertEqual(len(store.read("a", 1)), 1)

    def test_reconciliation_preserves_large_job_ids(self):
        for build in (InMemoryAllocationStore, _build_valkey_store):
            with self.subTest(store=build.__name__):
                store = build()
                job_id = 2**53 + 1
                _seed(store, "a", 1, job_id, ResourceUsage(4, 0, 0))
                self.assertTrue(store.admit_many(1, 1, [_admission("a", drop={job_id}), _admission("b")]))
                self.assertNotIn(job_id, store.read("a", 1))


class TestStoreParity(unittest.TestCase):
    def test_valkey_matches_in_memory(self):
        # fakeredis[lua] is a declared test dependency (see pyproject.toml). Fail hard if it is
        # missing rather than skipping, so the production Lua admit path can never drift from
        # _decide() unverified.
        self.assertIsNotNone(
            _build_valkey_store(),
            "fakeredis[lua] is required to run the store parity contract (install the 'test' "
            "extra); the Valkey Lua admit path must not go unverified.",
        )
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
