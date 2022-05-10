import os
import unittest
from vortex.rules import gateway
from . import mock_galaxy


class TestMapperSample(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool):
        galaxy_app = mock_galaxy.App()
        job = mock_galaxy.Job()
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')
        vortex_config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-sample.yml')
        gateway.ACTIVE_DESTINATION_MAPPER = None
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, vortex_config_files=[vortex_config])

    def test_map_sample_tool(self):
        tool = mock_galaxy.Tool('sometool')
        destination = self._map_to_destination(tool)
        self.assertEqual(destination.id, "local")
        self.assertEqual(destination.params['local_slots'], '2')
