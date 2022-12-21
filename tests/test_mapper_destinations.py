import os
import unittest
from tpv.rules import gateway
from tpv.core.test import mock_galaxy
from galaxy.jobs.mapper import JobMappingException


class TestMapperDestinations(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool, user, datasets, tpv_config_paths):
        galaxy_app = mock_galaxy.App(job_conf=os.path.join(os.path.dirname(__file__), 'fixtures/job_conf.yml'))
        job = mock_galaxy.Job()
        for d in datasets:
            job.add_input_dataset(d)
        gateway.ACTIVE_DESTINATION_MAPPER = None
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, tpv_config_files=tpv_config_paths)

    def test_destination_no_rule_match(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-destinations.yml')

        # an intermediate file size should compute correct values
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=7*1024**3))]
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config])
        self.assertEqual(destination.id, "k8s_environment")
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'TEST_JOB_SLOTS'], ['2'])
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'SPECIAL_FLAG'], ['first'])
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'DOCKER_ENABLED'], ['true'])
        self.assertEqual(destination.params['memory_requests'], '6')

    def test_destination_rule_match_once(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-destinations.yml')

        # an intermediate file size should compute correct values
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=12*1024**3))]
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config])
        self.assertEqual(destination.id, "another_k8s_environment")
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'TEST_JOB_SLOTS'], ['2'])
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'TEST_ENTITY_PRIORITY'], ['4'])
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'SPECIAL_FLAG'], ['second'])
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'DOCKER_ENABLED'], [])
        self.assertEqual(destination.params['memory_requests'], '12')

    def test_destination_rule_match_twice(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-destinations.yml')

        # an intermediate file size should compute correct values
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=22*1024**3))]
        with self.assertRaisesRegex(JobMappingException, "No destinations are available to fulfill request: bwa"):
            self._map_to_destination(tool, user, datasets, tpv_config_paths=[config])

    def test_destination_inheritance(self):
        tool = mock_galaxy.Tool('inheritance_test_tool')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-destinations.yml')

        # an intermediate file size should compute correct values
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=12*1024**3))]
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config])
        self.assertEqual(destination.id, "inherited_k8s_environment")
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'TEST_JOB_SLOTS'], ['2'])
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'TEST_ENTITY_PRIORITY'], ['4'])
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'TEST_ENTITY_GPUS'], ['0'])
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'SPECIAL_FLAG'], ['third'])
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'DOCKER_ENABLED'], [])
        self.assertEqual(destination.params['memory_requests'], '18')

    def test_destination_can_raise_not_ready_exception(self):
        tool = mock_galaxy.Tool('three_core_test_tool')
        user = mock_galaxy.User('tricia', 'tmcmillan@vortex.org')

        config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-destinations.yml')

        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=12*1024**3))]
        from galaxy.jobs.mapper import JobNotReadyException
        with self.assertRaises(JobNotReadyException):
            destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config])
            print(destination)

    def test_custom_destination_naming(self):
        tool = mock_galaxy.Tool('custom_tool')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-destinations.yml')

        # an intermediate file size should compute correct values
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=12*1024**3))]
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config])
        self.assertEqual(destination.id, "my-dest-with-2-cores-6-mem")

    def test_destination_with_handler_tags(self):
        tool = mock_galaxy.Tool('tool_with_handler_tags')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-destinations.yml')

        # an intermediate file size should compute correct values
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=12*1024**3))]
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config])
        self.assertEqual(destination.id, "destination_with_handler_tags")
        self.assertEqual(destination.tags, ["registered_user_concurrent_jobs_20"])

    def test_abstract_destinations_are_not_schedulable(self):
        tool = mock_galaxy.Tool('tool_matching_abstract_dest')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-destinations.yml')

        # an intermediate file size should compute correct values
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=12*1024**3))]
        with self.assertRaises(JobMappingException):
            self._map_to_destination(tool, user, datasets, tpv_config_paths=[config])

    def test_concrete_destinations_are_schedulable(self):
        tool = mock_galaxy.Tool('tool_matching_abstract_inherited_dest')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-destinations.yml')

        # an intermediate file size should compute correct values
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=12 * 1024 ** 3))]
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config])
        self.assertEqual(destination.id, "my_concrete_destination")

    def test_min_accepted_cpus_honoured(self):
        user = mock_galaxy.User('toolmatchuser', 'toolmatchuser@vortex.org')
        config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-destinations.yml')
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=12 * 1024 ** 3))]

        # First tool should not match
        tool = mock_galaxy.Tool('tool_for_testing_min_cpu_acceptance_non_match')
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config])
        self.assertEqual(destination.id, "destination_without_min_cpu_accepted")

        # second tool should match the min requirements for the destination
        tool = mock_galaxy.Tool('tool_for_testing_min_cpu_acceptance_match')
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config])
        self.assertEqual(destination.id, "destination_with_min_cpu_accepted")

    def test_min_accepted_gpus_honoured(self):
        user = mock_galaxy.User('toolmatchuser', 'toolmatchuser@vortex.org')
        config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-destinations.yml')
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=12 * 1024 ** 3))]

        # First tool should not match
        tool = mock_galaxy.Tool('tool_for_testing_min_gpu_acceptance_non_match')
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config])
        self.assertEqual(destination.id, "destination_without_min_gpu_accepted")

        # second tool should match the min requirements for the destination
        tool = mock_galaxy.Tool('tool_for_testing_min_gpu_acceptance_match')
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config])
        self.assertEqual(destination.id, "destination_with_min_gpu_accepted")

    def test_min_accepted_mem_honoured(self):
        user = mock_galaxy.User('toolmatchuser', 'toolmatchuser@vortex.org')
        config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-destinations.yml')
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=12 * 1024 ** 3))]

        # First tool should not match
        tool = mock_galaxy.Tool('tool_for_testing_min_mem_acceptance_non_match')
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config])
        self.assertEqual(destination.id, "destination_without_min_mem_accepted")

        # second tool should match the min requirements for the destination
        tool = mock_galaxy.Tool('tool_for_testing_min_mem_acceptance_match')
        destination = self._map_to_destination(tool, user, datasets, tpv_config_paths=[config])
        self.assertEqual(destination.id, "destination_with_min_mem_accepted")
