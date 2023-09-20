import os
import unittest
from tpv.rules import gateway
from tpv.commands.test import mock_galaxy
from tpv.core.loader import InvalidParentException


class TestMapperInheritance(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool, user, datasets, tpv_config_path=None, tpv_config_files = []):
        galaxy_app = mock_galaxy.App(job_conf=os.path.join(os.path.dirname(__file__), 'fixtures/job_conf.yml'))
        job = mock_galaxy.Job()
        for d in datasets:
            job.add_input_dataset(d)
        tpv_config = tpv_config_path or os.path.join(os.path.dirname(__file__),
                                                     'fixtures/mapping-inheritance.yml')
        if not tpv_config_files:
            tpv_config_files = [tpv_config]
        gateway.ACTIVE_DESTINATION_MAPPER = None
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, tpv_config_files=tpv_config_files)

    def test_map_inherit_twice(self):
        tool = mock_galaxy.Tool('trinity')
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5*1024**3))]

        destination = self._map_to_destination(tool, user, datasets)
        self.assertEqual(destination.id, "k8s_environment")
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'TEST_JOB_SLOTS'], ['4'])
        self.assertEqual(destination.params['native_spec'], '--mem 16 --cores 4 --gpus 3')

    def test_map_inherit_thrice(self):
        tool = mock_galaxy.Tool('hisat')
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5*1024**3))]

        destination = self._map_to_destination(tool, user, datasets)
        self.assertEqual(destination.id, "local")
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'TEST_JOB_SLOTS'], ['4'])
        self.assertEqual(destination.params['native_spec'], '--mem 16 --cores 4 --gpus 4')

    def test_map_inherit_invalid(self):
        tool = mock_galaxy.Tool('tophat')
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5*1024**3))]
        tpv_config_path = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-inheritance-invalid.yml')

        with self.assertRaises(InvalidParentException):
            self._map_to_destination(tool, user, datasets, tpv_config_path=tpv_config_path)

    def test_map_inherit_no_default(self):
        tool = mock_galaxy.Tool('hisat')
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5*1024**3))]
        tpv_config_path = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-inheritance-no-default.yml')

        destination = self._map_to_destination(tool, user, datasets, tpv_config_path=tpv_config_path)
        self.assertEqual(destination.id, "local")
        self.assertFalse([env['value'] for env in destination.env if env['name'] == 'TEST_JOB_SLOTS'])
        self.assertEqual(destination.params['another_spec'], '--gpus 4')

    def test_map_inherit_no_default_no_tool_def(self):
        tool = mock_galaxy.Tool('some_random_tool')
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')
        tpv_config_path = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-inheritance-no-default.yml')

        destination = self._map_to_destination(tool, user, datasets=[], tpv_config_path=tpv_config_path)
        self.assertEqual(destination.id, "local")

    # ref: https://github.com/galaxyproject/total-perspective-vortex/pull/68
    def test_map_with_default_tool_in_same_file(self):
        tool_id = 'toolshed.g2.bx.psu.edu/repos/bgruening/bionano_scaffold/bionano_scaffold/1.23a'
        user = mock_galaxy.User('majikthise', 'majikthise@vortex.org')
        tool = mock_galaxy.Tool(tool_id)
        tpv_config_files = [
            os.path.join(os.path.dirname(__file__), 'fixtures/default_tool/scenario-shared-rules.yml'),
            os.path.join(os.path.dirname(__file__), 'fixtures/default_tool/scenario-local-config-default-tool.yml'),
            os.path.join(os.path.dirname(__file__), 'fixtures/default_tool/scenario-local-config-tools.yml'),
            os.path.join(os.path.dirname(__file__), 'fixtures/default_tool/scenario-local-config-destinations.yml'),
        ]
        destination = self._map_to_destination(tool, user, datasets=[], tpv_config_files=tpv_config_files)
        self.assertEqual('--cores=8 --mem=250', destination.params.get('submit_native_specification'))

    # ref: https://github.com/galaxyproject/total-perspective-vortex/pull/68
    def test_map_with_default_rules_in_dedicated_file(self):
        tool_id = 'toolshed.g2.bx.psu.edu/repos/bgruening/bionano_scaffold/bionano_scaffold/1.23a'
        user = mock_galaxy.User('majikthise', 'majikthise@vortex.org')
        tool = mock_galaxy.Tool(tool_id)
        tpv_config_files = [
            os.path.join(os.path.dirname(__file__), 'fixtures/default_tool/scenario-shared-rules.yml'),
            os.path.join(os.path.dirname(__file__), 'fixtures/default_tool/scenario-local-config-tools-with-default.yml'),
            os.path.join(os.path.dirname(__file__), 'fixtures/default_tool/scenario-local-config-destinations.yml'),
        ]
        destination = self._map_to_destination(tool, user, datasets=[], tpv_config_files=tpv_config_files)
        self.assertEqual('--cores=8 --mem=250', destination.params.get('submit_native_specification'))

    def test_destination_inherits_runner_from_default(self):
        tool_id = 'kraken2'
        user = mock_galaxy.User('benjy', 'benjymouse@vortex.org')
        tool = mock_galaxy.Tool(tool_id)
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5*1024**3))]
        destination = self._map_to_destination(tool, user, datasets)
        self.assertEqual('destination_that_inherits_runner_from_default', destination.id)
        self.assertEqual('local', destination.runner)

    def test_general_destination_inheritance(self):
        tool_id = 'kraken5'
        user = mock_galaxy.User('frankie', 'frankiemouse@vortex.org')
        tool = mock_galaxy.Tool(tool_id)
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5*1024**3))]
        destination = self._map_to_destination(tool, user, datasets)
        self.assertEqual('destination_that_inherits_everything_from_k8s', destination.id)
        self.assertTrue('ABC' in [e.get('name') for e in destination.env])
        self.assertEqual('extra-args', destination.params.get('docker_extra'))
        self.assertEqual('k8s', destination.runner)
