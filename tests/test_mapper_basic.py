import os
import unittest
from vortex.rules import gateway
from . import mock_galaxy
from galaxy.jobs.mapper import JobMappingException


class TestMapperBasic(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool):
        galaxy_app = mock_galaxy.App()
        job = mock_galaxy.Job()
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')
        vortex_config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-basic.yml')
        gateway.ACTIVE_DESTINATION_MAPPER = None
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, vortex_config_files=[vortex_config])

    def test_map_default_tool(self):
        tool = mock_galaxy.Tool('sometool')
        destination = self._map_to_destination(tool)
        self.assertEqual(destination.id, "local")

    def test_map_overridden_tool(self):
        tool = mock_galaxy.Tool('bwa')
        destination = self._map_to_destination(tool)
        self.assertEqual(destination.id, "k8s_environment")

    def test_map_unschedulable_tool(self):
        tool = mock_galaxy.Tool('unschedulable_tool')
        with self.assertRaisesRegex(JobMappingException, "No destinations are available to fulfill request"):
            self._map_to_destination(tool)

    def test_map_invalidly_tagged_tool(self):
        tool = mock_galaxy.Tool('invalidly_tagged_tool')
        with self.assertRaisesRegex(JobMappingException, "No destinations are available to fulfill request"):
            self._map_to_destination(tool)

    def test_map_tool_by_regex(self):
        tool = mock_galaxy.Tool('regex_tool_test')
        destination = self._map_to_destination(tool)
        self.assertEqual(destination.id, "k8s_environment")

    def test_map_tool_by_regex_mismatch(self):
        tool = mock_galaxy.Tool('regex_t_test')
        destination = self._map_to_destination(tool)
        self.assertEqual(destination.id, "local")
