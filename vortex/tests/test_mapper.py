import os
import unittest
from vortex.rules import mapper
from . import mock_galaxy


class TestResourceParser(unittest.TestCase):

    _multiprocess_can_split_ = True

    def test_map_default_tool(self):
        galaxy_app = mock_galaxy.App()
        job = mock_galaxy.Job()
        tool = mock_galaxy.Tool('sometool')
        user = mock_galaxy.User('gargravarr', 'fairy_cake@totalperspectivevortex.galaxy')
        mapper_config = os.path.join(os.path.dirname(__file__), 'fixtures/rules-initial.yml')

        destination = mapper.map_tool_to_destination(galaxy_app, job, tool, user, mapper_config_file=mapper_config)
        self.assertEqual(destination.id, "local")

    def test_map_overridden_tool(self):
        galaxy_app = mock_galaxy.App()
        job = mock_galaxy.Job()
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('gargravarr', 'fairy_cake@totalperspectivevortex.galaxy')
        mapper_config = os.path.join(os.path.dirname(__file__), 'fixtures/rules-initial.yml')

        destination = mapper.map_tool_to_destination(galaxy_app, job, tool, user, mapper_config_file=mapper_config)
        self.assertEqual(destination.id, "k8s_environment")

    def test_map_unschedulable_tool(self):
        galaxy_app = mock_galaxy.App()
        job = mock_galaxy.Job()
        tool = mock_galaxy.Tool('unschedulable_tool')
        user = mock_galaxy.User('gargravarr', 'fairy_cake@totalperspectivevortex.galaxy')
        mapper_config = os.path.join(os.path.dirname(__file__), 'fixtures/rules-initial.yml')

        destination = mapper.map_tool_to_destination(galaxy_app, job, tool, user, mapper_config_file=mapper_config)
        self.assertIsNone(destination)

    def test_map_invalidly_tagged_tool(self):
        galaxy_app = mock_galaxy.App()
        job = mock_galaxy.Job()
        tool = mock_galaxy.Tool('invalidly_tagged_tool')
        user = mock_galaxy.User('gargravarr', 'fairy_cake@totalperspectivevortex.galaxy')
        mapper_config = os.path.join(os.path.dirname(__file__), 'fixtures/rules-initial.yml')

        destination = mapper.map_tool_to_destination(galaxy_app, job, tool, user, mapper_config_file=mapper_config)
        self.assertIsNone(destination)
