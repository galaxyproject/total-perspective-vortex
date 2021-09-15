import os
import time
import tempfile
import shutil
import unittest
from vortex.rules import gateway
from . import mock_galaxy


class TestScenarios(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool, user, mapping_rules_path=None):
        galaxy_app = mock_galaxy.App()
        job = mock_galaxy.Job()
        mapper_config = mapping_rules_path or os.path.join(os.path.dirname(__file__), 'fixtures/mapping-rules.yml')
        gateway.ACTIVE_DESTINATION_MAPPER = None
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, mapper_config_file=mapper_config)

    def test_scenario_node_marked_offline(self):
        with tempfile.NamedTemporaryFile('w+t') as tmp_file:
            rule_file = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-node-online.yml')
            shutil.copy2(rule_file, tmp_file.name)

            tool = mock_galaxy.Tool('bwa')
            user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')

            destination = self._map_to_destination(tool, user, mapping_rules_path=tmp_file.name)
            self.assertEqual(destination.id, "k8s_environment")

            # update the rule file with one node marked offline
            updated_rule_file = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-node-offline.yml')
            shutil.copy2(updated_rule_file, tmp_file.name)

            # wait for reload
            time.sleep(0.5)

            # should now map to the available node
            destination = self._map_to_destination(tool, user, mapping_rules_path=tmp_file.name)
            self.assertEqual(destination.id, "local")
