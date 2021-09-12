import os
import unittest
from vortex.rules import mapper
from . import mock_galaxy


class TestResourceParserBasic(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool):
        galaxy_app = mock_galaxy.App()
        job = mock_galaxy.Job()
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')
        mapper_config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-basic.yml')
        mapper.ACTIVE_DESTINATION_MAPPER = None
        return mapper.map_tool_to_destination(galaxy_app, job, tool, user, mapper_config_file=mapper_config)

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
        destination = self._map_to_destination(tool)
        self.assertIsNone(destination, f"{destination.id}" if destination else "")

    def test_map_invalidly_tagged_tool(self):
        tool = mock_galaxy.Tool('invalidly_tagged_tool')
        destination = self._map_to_destination(tool)
        self.assertIsNone(destination, f"{destination.id}" if destination else "")

    def test_map_tool_by_regex(self):
        tool = mock_galaxy.Tool('regex_tool_test')
        destination = self._map_to_destination(tool)
        self.assertEqual(destination.id, "k8s_environment")

    def test_map_tool_by_regex_mismatch(self):
        tool = mock_galaxy.Tool('regex_t_test')
        destination = self._map_to_destination(tool)
        self.assertEqual(destination.id, "local")