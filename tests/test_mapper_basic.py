import os
import re
import unittest

from galaxy.jobs.mapper import JobMappingException

from tpv.commands.test import mock_galaxy
from tpv.core.util import load_yaml_from_url_or_path
from tpv.rules import gateway


def map_to_destination(tool, **kwd):
    galaxy_app = mock_galaxy.App(job_conf=os.path.join(os.path.dirname(__file__), "fixtures/job_conf.yml"))
    job = mock_galaxy.Job()
    user = mock_galaxy.User("gargravarr", "fairycake@vortex.org")
    gateway.ACTIVE_DESTINATION_MAPPERS = {}
    return gateway.map_tool_to_destination(galaxy_app, job, tool, user, **kwd)


class TestMapperBasicFromPath(unittest.TestCase):

    @classmethod
    def _config_args(clazz, tpv_config_path=None):
        tpv_config_path = tpv_config_path or os.path.join(os.path.dirname(__file__), "fixtures/mapping-basic.yml")
        return dict(tpv_config_files=[tpv_config_path])

    def test_map_default_tool(self):
        tool = mock_galaxy.Tool("sometool")
        destination = map_to_destination(tool, **self._config_args())
        self.assertEqual(destination.id, "local")

    def test_map_overridden_tool(self):
        tool = mock_galaxy.Tool("bwa")
        destination = map_to_destination(tool, **self._config_args())
        self.assertEqual(destination.id, "k8s_environment")

    def test_map_unschedulable_tool(self):
        tool = mock_galaxy.Tool("unschedulable_tool")
        with self.assertRaisesRegex(JobMappingException, "No destinations are available to fulfill request"):
            map_to_destination(tool, **self._config_args())

    def test_map_invalidly_tagged_tool(self):
        tool = mock_galaxy.Tool("invalidly_tagged_tool")
        config = os.path.join(os.path.dirname(__file__), "fixtures/mapping-invalid-tags.yml")
        with self.assertRaisesRegex(Exception, r"Duplicate tags found: 'general' in \['require', 'reject'\]"):
            map_to_destination(tool, **self._config_args(config))

    def test_map_tool_by_regex(self):
        tool = mock_galaxy.Tool("regex_tool_test")
        destination = map_to_destination(tool, **self._config_args())
        self.assertEqual(destination.id, "k8s_environment")

    def test_map_tool_by_regex_mismatch(self):
        tool = mock_galaxy.Tool("regex_t_test")
        destination = map_to_destination(tool, **self._config_args())
        self.assertEqual(destination.id, "local")

    def test_map_tool_with_invalid_regex(self):
        tool = mock_galaxy.Tool("sometool")
        config = os.path.join(os.path.dirname(__file__), "fixtures/mapping-invalid-regex.yml")
        with self.assertRaisesRegex(re.error, "bad escape"):
            map_to_destination(tool, **self._config_args(config))

    def test_map_abstract_tool_should_fail(self):
        tool = mock_galaxy.Tool("my_abstract_tool")
        with self.assertRaisesRegex(JobMappingException, "This entity is abstract and cannot be mapped"):
            map_to_destination(tool, **self._config_args())

    def test_map_concrete_descendant_should_succeed(self):
        tool = mock_galaxy.Tool("my_concrete_tool")
        destination = map_to_destination(tool, **self._config_args())
        self.assertEqual(destination.id, "local")


class TestMapperBasicFromYaml(TestMapperBasicFromPath):
    @classmethod
    def _config_args(clazz, tpv_config_path=None):
        tpv_config_path = tpv_config_path or os.path.join(os.path.dirname(__file__), "fixtures/mapping-basic.yml")
        python_dict = load_yaml_from_url_or_path(tpv_config_path)
        return dict(tpv_configs=[python_dict])


class TestMapperConfigHandling(unittest.TestCase):

    def test_map_with_both_path_and_yaml_config(self):
        tool = mock_galaxy.Tool("my_concrete_tool")
        tpv_config_path = os.path.join(os.path.dirname(__file__), "fixtures/mapping-basic.yml")
        python_dict = load_yaml_from_url_or_path(tpv_config_path)
        with self.assertRaisesRegex(ValueError, "Only one of tpv_configs or tpv_config_files can be specified"):
            map_to_destination(tool, tpv_config_files=tpv_config_path, tpv_configs=[python_dict])

    def test_map_with_no_config(self):
        tool = mock_galaxy.Tool("my_concrete_tool")
        with self.assertRaisesRegex(ValueError, "One of tpv_configs or tpv_config_files must be specified"):
            map_to_destination(tool, tpv_config_files=None, tpv_configs=None)
