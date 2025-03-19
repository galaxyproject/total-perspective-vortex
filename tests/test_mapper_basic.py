import os
import re
import unittest
from tpv.rules import gateway
from tpv.commands.test import mock_galaxy
from galaxy.jobs.mapper import JobMappingException


class TestMapperBasic(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool, tpv_config_path=None):
        galaxy_app = mock_galaxy.App(job_conf=os.path.join(os.path.dirname(__file__), 'fixtures/job_conf.yml'))
        job = mock_galaxy.Job()
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')
        tpv_config = tpv_config_path or os.path.join(os.path.dirname(__file__),
                                                     'fixtures/mapping-basic.yml')
        gateway.ACTIVE_DESTINATION_MAPPERS = {}
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, tpv_config_files=[tpv_config])

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

    def test_map_tool_with_invalid_regex(self):
        tool = mock_galaxy.Tool('sometool')
        config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-invalid-regex.yml')
        with self.assertRaisesRegex(re.error, "unterminated character set"):
            self._map_to_destination(tool, tpv_config_path=config)

    def test_map_abstract_tool_should_fail(self):
        tool = mock_galaxy.Tool('my_abstract_tool')
        with self.assertRaisesRegex(JobMappingException, "This entity is abstract and cannot be mapped"):
            self._map_to_destination(tool)

    def test_map_concrete_descendant_should_succeed(self):
        tool = mock_galaxy.Tool('my_concrete_tool')
        destination = self._map_to_destination(tool)
        self.assertEqual(destination.id, "local")
