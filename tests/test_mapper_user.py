import os
import unittest
from tpv.rules import gateway
from tpv.core.entities import IncompatibleTagsException
from tpv.core.test import mock_galaxy


class TestMapperUser(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool, user):
        galaxy_app = mock_galaxy.App(job_conf=os.path.join(os.path.dirname(__file__), 'fixtures/job_conf.yml'))
        job = mock_galaxy.Job()
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-user.yml')
        gateway.ACTIVE_DESTINATION_MAPPER = None
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, tpv_config_files=[tpv_config])

    def test_map_default_user(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('ford', 'ford@vortex.org')

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
        user = mock_galaxy.User('ford', 'ford@vortex.org')

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

    def test_tool_below_min_resources_for_user(self):
        tool = mock_galaxy.Tool('tool_below_min_resources')
        user = mock_galaxy.User('prefect', 'prefect@vortex.org')

        destination = self._map_to_destination(tool, user)
        self.assertEqual(destination.id, "special_resource_environment")
        # should use the lower of the two core and special_resource_environment values for this user
        self.assertEqual(destination.params['native_spec'], '--mem 16 --cores 2 --gpus 2')

    def test_tool_above_max_resources_for_user(self):
        tool = mock_galaxy.Tool('tool_above_max_resources')
        user = mock_galaxy.User('prefect', 'prefect@vortex.org')

        destination = self._map_to_destination(tool, user)
        self.assertEqual(destination.id, "special_resource_environment")
        # should use the lower of the two core and mem values for this user
        self.assertEqual(destination.params['native_spec'], '--mem 32 --cores 4 --gpus 3')
