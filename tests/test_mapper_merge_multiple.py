import os
import unittest
from vortex.rules import gateway
from . import mock_galaxy
from galaxy.jobs.mapper import JobMappingException


class TestMapperMergeMultipleConfigs(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool, user, datasets, vortex_config_paths):
        galaxy_app = mock_galaxy.App()
        job = mock_galaxy.Job()
        for d in datasets:
            job.add_input_dataset(d)
        gateway.ACTIVE_DESTINATION_MAPPER = None
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, vortex_config_files=vortex_config_paths)

    def test_merge_remote_and_local(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        config_first = "https://github.com/galaxyproject/total-perspective-vortex/raw/main/" \
                       "tests/fixtures/mapping-merge-multiple-remote.yml"
        config_second = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-merge-multiple-local.yml')

        # a small file size should fail because of remote rule
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=1*1024**3))]
        with self.assertRaisesRegex(JobMappingException, "We don't run piddling datasets"):
            self._map_to_destination(tool, user, datasets, vortex_config_paths=[config_first, config_second])

        # a large file size should fail because of local rule
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=25*1024**3))]
        with self.assertRaisesRegex(JobMappingException, "Too much data, shouldn't run"):
            self._map_to_destination(tool, user, datasets, vortex_config_paths=[config_first, config_second])

        # an intermediate file size should compute correct values
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=7*1024**3))]
        destination = self._map_to_destination(tool, user, datasets, vortex_config_paths=[config_first, config_second])
        self.assertEqual(destination.id, "k8s_environment")
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'TEST_JOB_SLOTS'], ['2'])
        self.assertEqual(destination.params['native_spec'], '--mem 8 --cores 2')

    def test_merge_local_with_local(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        config_first = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-merge-multiple-remote.yml')
        config_second = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-merge-multiple-local.yml')

        # a small file size should fail because of remote rule
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=1*1024**3))]
        with self.assertRaisesRegex(JobMappingException, "We don't run piddling datasets"):
            self._map_to_destination(tool, user, datasets, vortex_config_paths=[config_first, config_second])

        # a large file size should fail because of local rule
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=25*1024**3))]
        with self.assertRaisesRegex(JobMappingException, "Too much data, shouldn't run"):
            self._map_to_destination(tool, user, datasets, vortex_config_paths=[config_first, config_second])

        # an intermediate file size should compute correct values
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=7*1024**3))]
        destination = self._map_to_destination(tool, user, datasets, vortex_config_paths=[config_first, config_second])
        self.assertEqual(destination.id, "k8s_environment")
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'TEST_JOB_SLOTS'], ['2'])
        self.assertEqual(destination.params['native_spec'], '--mem 8 --cores 2')

    def test_merge_rules(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        config_first = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-merge-multiple-remote.yml')
        config_second = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-merge-multiple-local.yml')

        # the highmem rule should take effect
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=42*1024**3))]
        with self.assertRaisesRegex(JobMappingException, "a different kind of error"):
            self._map_to_destination(tool, user, datasets, vortex_config_paths=[config_first])

        # the highmem rule should not take effect for this size, as we've overridden it
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=42*1024**3))]
        destination = self._map_to_destination(tool, user, datasets, vortex_config_paths=[config_first, config_second])
        self.assertEqual(destination.id, "another_k8s_environment")
