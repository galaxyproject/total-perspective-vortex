import os
import unittest

from tpv.commands.test import mock_galaxy
from tpv.rules import gateway


class TestParamsSpecific(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool, user):
        galaxy_app = mock_galaxy.App(job_conf=os.path.join(os.path.dirname(__file__), "fixtures/job_conf.yml"))
        job = mock_galaxy.Job()
        tpv_config = os.path.join(os.path.dirname(__file__), "fixtures/mapping-params-specific.yml")
        gateway.ACTIVE_DESTINATION_MAPPERS = {}
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, tpv_config_files=[tpv_config])

    def test_default_does_not_inherit_descendant_params(self):
        tool = mock_galaxy.Tool("bwa")
        user = mock_galaxy.User("ford", "prefect@vortex.org")

        destination = self._map_to_destination(tool, user)
        self.assertTrue("earth" not in destination.params)

    def test_default_does_not_inherit_descendant_env(self):
        tool = mock_galaxy.Tool("agrajag")
        user = mock_galaxy.User("ford", "prefect@vortex.org")

        destination = self._map_to_destination(tool, user)
        self.assertTrue("JAVA_MEM" not in [e["name"] for e in destination.env])

    def test_map_complex_parameter(self):
        tool = mock_galaxy.Tool("bwa")
        user = mock_galaxy.User("gargravarr", "fairycake@vortex.org")

        destination = self._map_to_destination(tool, user)
        self.assertEqual(
            destination.params["container_override"][0]["identifier"],
            "busybox:ubuntu-14.04-2",
        )

    def test_env_with_int_value_is_converted_to_string(self):
        tool = mock_galaxy.Tool("grappa")
        user = mock_galaxy.User("gargravarr", "fairycake@vortex.org")

        destination = self._map_to_destination(tool, user)
        self.assertEqual(type(destination.env[1]["value"]), str)
        self.assertEqual(destination.env[1]["value"], "42")

    def test_param_with_int_or_bool_value_is_not_converted_to_string(self):
        tool = mock_galaxy.Tool("grappa")
        user = mock_galaxy.User("gargravarr", "fairycake@vortex.org")

        destination = self._map_to_destination(tool, user)
        self.assertEqual(type(destination.params["is_a_bool"]), bool)
        self.assertEqual(destination.params["is_a_bool"], True)
        self.assertEqual(destination.params["int_value"], 1010)
