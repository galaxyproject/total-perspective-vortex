import os
import unittest
from tpv.rules import gateway
from . import mock_galaxy


class TestParamsSpecific(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool, user):
        galaxy_app = mock_galaxy.App()
        job = mock_galaxy.Job()
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-params-specific.yml')
        gateway.ACTIVE_DESTINATION_MAPPER = None
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, tpv_config_files=[tpv_config])

    def test_default_does_not_inherit_descendant_params(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        destination = self._map_to_destination(tool, user)
        self.assertTrue('earth' not in destination.params)

    def test_default_does_not_inherit_descendant_env(self):
        tool = mock_galaxy.Tool('agrajag')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        destination = self._map_to_destination(tool, user)
        self.assertTrue('JAVA_MEM' not in [e['name'] for e in destination.env])

    def test_map_complex_parameter(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')

        destination = self._map_to_destination(tool, user)
        self.assertEqual(destination.params['container_override'][0]['identifier'], 'busybox:ubuntu-14.04-2')
