import os
import unittest
from vortex.rules import gateway
from . import mock_galaxy


class TestMapperRank(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool, user):
        galaxy_app = mock_galaxy.App()
        job = mock_galaxy.Job()
        vortex_config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-rank.yml')
        gateway.ACTIVE_DESTINATION_MAPPER = None
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, vortex_config_files=[vortex_config])

    def test_map_custom_rank(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        destination = self._map_to_destination(tool, user)
        self.assertEqual(destination.id, "k8s_environment")

    def test_map_default_rank_but_with_preference(self):
        tool = mock_galaxy.Tool('trinity')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        destination = self._map_to_destination(tool, user)
        self.assertEqual(destination.id, "another_k8s_environment")
