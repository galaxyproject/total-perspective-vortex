import os
import unittest

from tpv.commands.dryrunner import TPVDryRunner
from tpv.commands.test import mock_galaxy
from tpv.core.resource_pool import InMemoryAllocationStore
from tpv.rules import gateway

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
POOL_CONFIG = os.path.join(FIXTURES, "mapping-resource-pool.yml")
DEAD_STORE_OVERLAY = os.path.join(FIXTURES, "mapping-resource-pool-dead-store.yml")
JOB_CONF = os.path.join(FIXTURES, "job_conf_dry_run.yml")


class TestMockJob(unittest.TestCase):
    def test_jobs_get_distinct_default_ids(self):
        # Pool admission keys the ledger by job id, so a mock job must always have one.
        a, b = mock_galaxy.Job(), mock_galaxy.Job()
        self.assertIsInstance(a.id, int)
        self.assertNotEqual(a.id, b.id)


class TestDryRunResourcePools(unittest.TestCase):
    def _run(self, tool, user, confs, explain=False):
        runner = TPVDryRunner.from_params(job_conf=JOB_CONF, tool_id=tool, user_email=user, tpv_confs=confs)
        return runner.run(explain=explain)

    def test_dry_run_maps_a_pooled_config(self):
        destination, _ = self._run("bigtool", "arthur@vortex.org", [POOL_CONFIG])
        self.assertEqual(destination.id, "local")

    def test_dry_run_never_touches_the_configured_store(self):
        # The configured store is a Valkey nothing listens on. If the dry run consulted it, the
        # job would be deferred fail-closed (or the run would error); an admin's dry run must
        # neither depend on nor write to production accounting.
        destination, _ = self._run("bigtool", "arthur@vortex.org", [POOL_CONFIG, DEAD_STORE_OVERLAY])
        self.assertEqual(destination.id, "local")
        store = gateway.ACTIVE_DESTINATION_MAPPERS["tpv_dispatcher"].resource_pools.store
        self.assertIsInstance(store, InMemoryAllocationStore)

    def test_dry_run_still_reports_pools_without_a_store(self):
        # Swapping in an in-memory store must not hide the misconfiguration the mapper rejects.
        no_store = os.path.join(FIXTURES, "mapping-resource-pool-no-store.yml")
        with self.assertRaisesRegex(ValueError, "resource_pool_store"):
            self._run("bigtool", "arthur@vortex.org", [no_store])

    def test_explain_reports_which_pools_govern_the_job_and_why(self):
        _, collector = self._run("bigtool", "arthur@vortex.org", [POOL_CONFIG], explain=True)
        trace = collector.render()
        self.assertIn("Resource Pools", trace)
        # bigtool is 64 cores against a 32-core budget: governed by 'default', classified oversize.
        self.assertIn("default", trace)
        self.assertIn("oversize", trace.lower())
        self.assertIn("32", trace)

    def test_explain_reports_a_permanently_rejected_request(self):
        # hugetool (200 cores) exceeds the oversize ceiling and can never be scheduled.
        destination, collector = self._run("hugetool", "arthur@vortex.org", [POOL_CONFIG], explain=True)
        self.assertIsNone(destination)
        self.assertIn("can never be scheduled", collector.render())
