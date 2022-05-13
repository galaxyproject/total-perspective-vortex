import os
import unittest
from vortex.rules import gateway
from . import mock_galaxy
from vortex.core.loader import InvalidParentException


class TestMapperInheritance(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool, user, datasets, vortex_config_path=None):
        galaxy_app = mock_galaxy.App()
        job = mock_galaxy.Job()
        for d in datasets:
            job.add_input_dataset(d)
        vortex_config = vortex_config_path or os.path.join(os.path.dirname(__file__),
                                                           'fixtures/mapping-inheritance.yml')
        gateway.ACTIVE_DESTINATION_MAPPER = None
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, vortex_config_files=[vortex_config])

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
        vortex_config_path = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-inheritance-invalid.yml')

        with self.assertRaises(InvalidParentException):
            self._map_to_destination(tool, user, datasets, vortex_config_path=vortex_config_path)

    def test_map_inherit_no_default(self):
        tool = mock_galaxy.Tool('hisat')
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5*1024**3))]
        vortex_config_path = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-inheritance-no-default.yml')

        destination = self._map_to_destination(tool, user, datasets, vortex_config_path=vortex_config_path)
        self.assertEqual(destination.id, "local")
        self.assertFalse([env['value'] for env in destination.env if env['name'] == 'TEST_JOB_SLOTS'])
        self.assertEqual(destination.params['another_spec'], '--gpus 4')

    def test_map_inherit_no_default_no_tool_def(self):
        tool = mock_galaxy.Tool('some_random_tool')
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')
        vortex_config_path = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-inheritance-no-default.yml')

        destination = self._map_to_destination(tool, user, datasets=[], vortex_config_path=vortex_config_path)
        self.assertEqual(destination.id, "local")
