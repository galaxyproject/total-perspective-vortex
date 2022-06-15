import os
import unittest
from tpv.rules import gateway
from . import mock_galaxy
from tpv.core.loader import InvalidParentException


class TestMapperContext(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool, user, datasets, tpv_config_path=None):
        galaxy_app = mock_galaxy.App()
        job = mock_galaxy.Job()
        for d in datasets:
            job.add_input_dataset(d)
        tpv_config = tpv_config_path or os.path.join(os.path.dirname(__file__),
                                                     'fixtures/mapping-context.yml')
        gateway.ACTIVE_DESTINATION_MAPPER = None
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, tpv_config_files=[tpv_config])

    def test_map_context_default_overrides_global(self):
        tool = mock_galaxy.Tool('trinity')
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5*1024**3))]

        destination = self._map_to_destination(tool, user, datasets)
        self.assertEqual(destination.id, "local")
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'TEST_JOB_SLOTS'], ['3'])
        self.assertEqual(destination.params['native_spec'], '--mem 9 --cores 3 --gpus 3')

    def test_map_tool_overrides_default(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5*1024**3))]

        destination = self._map_to_destination(tool, user, datasets)
        self.assertEqual(destination.id, "k8s_environment")
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'TEST_JOB_SLOTS'], ['5'])
        self.assertEqual(destination.params['native_spec'], '--mem 15 --cores 5 --gpus 4')
