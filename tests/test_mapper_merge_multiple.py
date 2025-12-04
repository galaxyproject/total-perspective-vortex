import os
import unittest

from galaxy.jobs.mapper import JobMappingException

from tpv.commands.test import mock_galaxy
from tpv.core.loader import InvalidParentException
from tpv.rules import gateway


class TestMapperMergeMultipleConfigs(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool, user, datasets, tpv_config_paths):
        galaxy_app = mock_galaxy.App(job_conf=os.path.join(os.path.dirname(__file__), "fixtures/job_conf.yml"))
        job = mock_galaxy.Job()
        for d in datasets:
            job.add_input_dataset(d)
        gateway.ACTIVE_DESTINATION_MAPPERS = {}
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, tpv_config_files=tpv_config_paths)

    def test_merge_remote_and_local(self):
        tool = mock_galaxy.Tool("bwa")
        user = mock_galaxy.User("ford", "prefect@vortex.org")

        config_first = (
            "https://github.com/galaxyproject/total-perspective-vortex/raw/main/"
            "tests/fixtures/mapping-merge-multiple-remote.yml"
        )
        config_second = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-local.yml")

        # a small file size should fail because of remote rule
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=1 * 1024**3))]
        with self.assertRaisesRegex(JobMappingException, "We don't run piddling datasets"):
            self._map_to_destination(tool, user, datasets, tpv_config_paths=[config_first, config_second])

        # a large file size should fail because of local rule
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=25 * 1024**3))]
        with self.assertRaisesRegex(JobMappingException, "Too much data, shouldn't run"):
            self._map_to_destination(tool, user, datasets, tpv_config_paths=[config_first, config_second])

        # an intermediate file size should compute correct values
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=7 * 1024**3))]
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config_first, config_second])
        self.assertEqual(destination.id, "k8s_environment")
        self.assertEqual(
            [env["value"] for env in destination.env if env["name"] == "TEST_JOB_SLOTS"],
            ["4"],
        )
        self.assertEqual(destination.params["native_spec"], "--mem 8 --cores 2")
        self.assertEqual(destination.params["custom_context_remote"], "remote var")
        self.assertEqual(destination.params["custom_context_local"], "local var")
        self.assertEqual(destination.params["custom_context_override"], "local override")

    def test_merge_local_with_local(self):
        tool = mock_galaxy.Tool("bwa")
        user = mock_galaxy.User("ford", "prefect@vortex.org")

        config_first = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-remote.yml")
        config_second = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-local.yml")

        # a small file size should fail because of remote rule
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=1 * 1024**3))]
        with self.assertRaisesRegex(JobMappingException, "We don't run piddling datasets"):
            self._map_to_destination(tool, user, datasets, tpv_config_paths=[config_first, config_second])

        # a large file size should fail because of local rule
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=25 * 1024**3))]
        with self.assertRaisesRegex(JobMappingException, "Too much data, shouldn't run"):
            self._map_to_destination(tool, user, datasets, tpv_config_paths=[config_first, config_second])

        # an intermediate file size should compute correct values
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=7 * 1024**3))]
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config_first, config_second])
        self.assertEqual(destination.id, "k8s_environment")
        self.assertEqual(
            [env["value"] for env in destination.env if env["name"] == "TEST_JOB_SLOTS"],
            ["4"],
        )
        self.assertEqual(destination.params["native_spec"], "--mem 8 --cores 2")
        self.assertEqual(destination.params["custom_context_remote"], "remote var")
        self.assertEqual(destination.params["custom_context_local"], "local var")
        self.assertEqual(destination.params["custom_context_override"], "local override")

    def test_merge_rules(self):
        tool = mock_galaxy.Tool("bwa")
        user = mock_galaxy.User("ford", "prefect@vortex.org")

        config_first = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-remote.yml")
        config_second = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-local.yml")

        # the highmem rule should take effect
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=42 * 1024**3))]
        with self.assertRaisesRegex(JobMappingException, "a different kind of error"):
            self._map_to_destination(tool, user, datasets, tpv_config_paths=[config_first])

        # the highmem rule should not take effect for this size, as we've overridden it
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=42 * 1024**3))]
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config_first, config_second])
        self.assertEqual(destination.id, "another_k8s_environment")

    def test_merge_rules_with_multiple_matches(self):
        tool = mock_galaxy.Tool("hisat2")
        user = mock_galaxy.User("ford", "prefect@vortex.org")

        config_first = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-remote.yml")
        config_second = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-local.yml")

        # the highmem rule should take effect, with local override winning
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=42 * 1024**3))]
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config_first, config_second])
        self.assertEqual(destination.id, "another_k8s_environment")
        # since the last defined hisat2 contains overridden defaults, those defaults will apply
        self.assertEqual(
            [env["value"] for env in destination.env if env["name"] == "TEST_JOB_SLOTS"],
            ["6"],
        )
        # this var is not overridden by the last defined defaults, and therefore, the remote value of cores*2 applies
        self.assertEqual(
            [env["value"] for env in destination.env if env["name"] == "MORE_JOB_SLOTS"],
            ["12"],
        )
        self.assertEqual(destination.params["native_spec"], "--mem 18 --cores 6")

    def test_merge_rules_local_defaults_do_not_override_remote_tool(self):
        tool = mock_galaxy.Tool("toolshed.g2.bx.psu.edu/repos/iuc/disco/disco/v1.0")
        user = mock_galaxy.User("ford", "prefect@vortex.org")

        config_first = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-remote.yml")
        config_second = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-local.yml")

        # the disco rules should take effect, with local override winning
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=42 * 1024**3))]
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config_first, config_second])
        self.assertEqual(destination.id, "k8s_environment")
        # since the last defined hisat2 contains overridden defaults, those defaults will apply
        self.assertEqual(
            [env["value"] for env in destination.env if env["name"] == "DISCO_MAX_MEMORY"],
            ["24"],
        )
        self.assertEqual(
            [env["value"] for env in destination.env if env["name"] == "DISCO_MORE_PARAMS"],
            ["just another param"],
        )
        # this var is not overridden by the last defined defaults, and therefore, the remote value applies
        self.assertEqual(destination.params["native_spec"], "--mem 24 --cores 8")

    def test_merge_remote_tool_overridden_by_local_inherited_tool(self):
        tool = mock_galaxy.Tool("remote_tool_inherit_test")
        user = mock_galaxy.User("ford", "prefect@vortex.org")

        config_first = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-remote.yml")
        config_second = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-local.yml")

        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=7 * 1024**3))]
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config_first, config_second])

        self.assertEqual(destination.id, "k8s_environment")
        # local tool inherits from another local tool and should override the remote definition
        self.assertEqual(destination.params["native_spec"], "--mem 15 --cores 5 --gpus 3")
        self.assertEqual(
            [env["value"] for env in destination.env if env["name"] == "LOCAL_PARENT"],
            ["5"],
        )
        self.assertEqual(
            [env["value"] for env in destination.env if env["name"] == "LOCAL_OVERRIDE"],
            ["yes"],
        )
        self.assertEqual(
            [env["value"] for env in destination.env if env["name"] == "REMOTE_MARKER"],
            ["remote"],
        )

    def test_merge_remote_tool_with_deep_local_inheritance_chain(self):
        tool = mock_galaxy.Tool("remote_tool_deep")
        user = mock_galaxy.User("ford", "prefect@vortex.org")

        config_first = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-remote.yml")
        config_second = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-local.yml")

        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=7 * 1024**3))]
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config_first, config_second])

        self.assertEqual(destination.id, "k8s_environment")
        # remote is grafted at the base, but local parents still override cores/mem
        self.assertEqual(destination.params["native_spec"], "--mem 21 --cores 7 --gpus 3")
        self.assertEqual(
            [env["value"] for env in destination.env if env["name"] == "TEST_JOB_SLOTS"],
            ["7"],
        )
        self.assertEqual(
            [env["value"] for env in destination.env if env["name"] == "ROOT_MARKER"],
            ["7"],
        )
        self.assertEqual(
            [env["value"] for env in destination.env if env["name"] == "MID_MARKER"],
            ["mid"],
        )
        self.assertEqual(
            [env["value"] for env in destination.env if env["name"] == "LEAF_MARKER"],
            ["leaf"],
        )
        self.assertEqual(
            [env["value"] for env in destination.env if env["name"] == "LOCAL_DEEP_OVERRIDE"],
            ["yes"],
        )
        self.assertEqual(
            [env["value"] for env in destination.env if env["name"] == "REMOTE_DEEP"],
            ["remote"],
        )

    def test_shared_local_base_with_distinct_remote_parents(self):
        config_first = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-remote.yml")
        config_second = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-local.yml")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=7 * 1024**3))]

        # First child should graft its matching remote but keep local base overrides
        tool_one = mock_galaxy.Tool("remote_child_one")
        destination = self._map_to_destination(
            tool_one,
            mock_galaxy.User("ford", "prefect@vortex.org"),
            datasets,
            tpv_config_paths=[config_first, config_second],
        )
        self.assertEqual(destination.id, "k8s_environment")
        self.assertEqual(destination.params["native_spec"], "--mem 10.0 --cores 4")
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "BASE_MARKER"], ["base"])
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "CHILD_ONE"], ["local_one"])
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "REMOTE_CHILD_ONE"], ["one"])
        self.assertFalse([env for env in destination.env if env["name"] == "REMOTE_CHILD_TWO"])

        # Second child should graft its own remote without leaking the first remote
        tool_two = mock_galaxy.Tool("remote_child_two")
        destination = self._map_to_destination(
            tool_two,
            mock_galaxy.User("ford", "prefect@vortex.org"),
            datasets,
            tpv_config_paths=[config_first, config_second],
        )
        self.assertEqual(destination.id, "k8s_environment")
        self.assertEqual(destination.params["native_spec"], "--mem 12 --cores 4")
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "BASE_MARKER"], ["base"])
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "CHILD_TWO"], ["local_two"])
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "REMOTE_CHILD_TWO"], ["two"])
        self.assertFalse([env for env in destination.env if env["name"] == "REMOTE_CHILD_ONE"])

    def test_shared_local_base_with_distinct_remote_parents_deep_chain(self):
        config_first = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-remote.yml")
        config_second = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-local.yml")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=7 * 1024**3))]

        tool_one = mock_galaxy.Tool("remote_deep_one")
        destination = self._map_to_destination(
            tool_one,
            mock_galaxy.User("ford", "prefect@vortex.org"),
            datasets,
            tpv_config_paths=[config_first, config_second],
        )
        self.assertEqual(destination.id, "k8s_environment")
        self.assertEqual(destination.params["native_spec"], "--mem 15.0 --cores 6 --gpus 1")
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "BASE_DEEP"], ["base_deep"])
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "MID_DEEP"], ["mid_deep"])
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "LEAF_DEEP_ONE"], ["leaf_one"])
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "LOCAL_DEEP_ONE"], ["yes"])
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "REMOTE_DEEP_ONE"], ["one"])
        self.assertFalse([env for env in destination.env if env["name"] == "REMOTE_DEEP_TWO"])

        tool_two = mock_galaxy.Tool("remote_deep_two")
        destination = self._map_to_destination(
            tool_two,
            mock_galaxy.User("ford", "prefect@vortex.org"),
            datasets,
            tpv_config_paths=[config_first, config_second],
        )
        self.assertEqual(destination.id, "k8s_environment")
        self.assertEqual(destination.params["native_spec"], "--mem 16.799999999999997 --cores 6 --gpus 2")
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "BASE_DEEP"], ["base_deep"])
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "MID_DEEP"], ["mid_deep"])
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "LEAF_DEEP_TWO"], ["leaf_two"])
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "LOCAL_DEEP_TWO"], ["yes"])
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "REMOTE_DEEP_TWO"], ["two"])
        self.assertFalse([env for env in destination.env if env["name"] == "REMOTE_DEEP_ONE"])

    def test_remote_prior_with_deep_chain(self):
        config_first = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-remote.yml")
        config_second = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-local.yml")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=7 * 1024**3))]

        tool = mock_galaxy.Tool("remote_prior_child")
        destination = self._map_to_destination(
            tool,
            mock_galaxy.User("ford", "prefect@vortex.org"),
            datasets,
            tpv_config_paths=[config_first, config_second],
        )
        self.assertEqual(destination.id, "k8s_environment")
        # local deep chain sets cores/mem, remote contributes gpus and remote markers
        self.assertEqual(destination.params["native_spec"], "--mem 17.5 --cores 5 --gpus 2")
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "LOCAL_PRIOR_BASE"], ["base"])
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "LOCAL_PRIOR_MID"], ["mid"])
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "LOCAL_PRIOR_LEAF"], ["leaf"])
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "REMOTE_PRIOR_ROOT"], ["root"])
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "REMOTE_PRIOR_MID"], ["mid"])
        self.assertEqual([env["value"] for env in destination.env if env["name"] == "REMOTE_PRIOR_LEAF"], ["leaf"])

    def test_missing_local_parent_raises(self):
        tool = mock_galaxy.Tool("remote_missing_parent")
        user = mock_galaxy.User("ford", "prefect@vortex.org")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=7 * 1024**3))]

        config_first = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-remote.yml")
        config_second = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-missing-parent-local.yml")

        with self.assertRaisesRegex(
            InvalidParentException, "The specified parent: missing_local_parent for entity: remote_missing_parent"
        ):
            self._map_to_destination(tool, user, datasets, tpv_config_paths=[config_first, config_second])

    def test_inheritance_cycle_raises(self):
        tool = mock_galaxy.Tool("remote_cycle_tool")
        user = mock_galaxy.User("ford", "prefect@vortex.org")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=7 * 1024**3))]

        config_first = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-multiple-remote.yml")
        config_second = os.path.join(os.path.dirname(__file__), "fixtures/mapping-merge-cycle-local.yml")

        with self.assertRaisesRegex(
            InvalidParentException,
            r"Cycle detected in inheritance chain for entity: remote_cycle_tool",
        ):
            self._map_to_destination(tool, user, datasets, tpv_config_paths=[config_first, config_second])
