import os
import unittest
from vortex.rules import gateway
from vortex.core.entities import IncompatibleTagsException
from . import mock_galaxy


class TestMapperUser(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool, user):
        galaxy_app = mock_galaxy.App()
        job = mock_galaxy.Job()
        vortex_config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-user.yml')
        gateway.ACTIVE_DESTINATION_MAPPER = None
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, vortex_config_files=[vortex_config])

    def test_map_default_user(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        destination = self._map_to_destination(tool, user)
        self.assertEqual(destination.id, "k8s_environment")

    def test_map_overridden_user(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')

        destination = self._map_to_destination(tool, user)
        self.assertEqual(destination.id, "k8s_environment")

    def test_map_unschedulable_user(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('arthur', 'arthur@vortex.org')

        with self.assertRaises(IncompatibleTagsException):
            self._map_to_destination(tool, user)

    def test_map_invalidly_tagged_user(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('infinitely', 'improbable@vortex.org')

        with self.assertRaises(IncompatibleTagsException):
            self._map_to_destination(tool, user)

    def test_map_user_by_regex(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')

        destination = self._map_to_destination(tool, user)
        self.assertEqual(destination.id, "k8s_environment")

    def test_map_user_by_regex_mismatch(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('gargravarr', 'fairycake@notvortex.org')

        with self.assertRaises(IncompatibleTagsException):
            self._map_to_destination(tool, user)

    def test_map_user_entity_usage_scenario_1(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        destination = self._map_to_destination(tool, user)
        self.assertEqual(destination.id, "k8s_environment")
        # should use the lower of the two core and mem values for this user
        self.assertEqual(destination.params['native_spec'], '--mem 4 --cores 2')

    def test_map_user_entity_usage_scenario_2(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('ford', 'fairycake@vortex.org')

        destination = self._map_to_destination(tool, user)
        self.assertEqual(destination.id, "k8s_environment")
        # should use the lower of the two core and mem values for this user
        self.assertEqual(destination.params['native_spec'], '--mem 8 --cores 1')
