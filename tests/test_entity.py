import os
import unittest
from tpv.rules import gateway
from . import mock_galaxy


class TestEntity(unittest.TestCase):

    @staticmethod
    def _map_to_destination(app, job, tool, user):
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-rule-argument-based.yml')
        gateway.ACTIVE_DESTINATION_MAPPER = None
        return gateway.map_tool_to_destination(app, job, tool, user, tpv_config_files=[tpv_config])

    # issue: https://github.com/galaxyproject/total-perspective-vortex/issues/53
    def test_all_entities_refer_to_same_loader(self):
        app = mock_galaxy.App()
        job = mock_galaxy.Job()

        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        # just map something so the ACTIVE_DESTINATION_MAPPER is populated
        self._map_to_destination(app, job, tool, user)

        # get the original loader
        original_loader = gateway.ACTIVE_DESTINATION_MAPPER.loader

        # make sure we are still referring to the same loader after evaluation
        _, evaluated_entity = gateway.ACTIVE_DESTINATION_MAPPER.match_combine_evaluate_entities(app, tool, user, job)
        assert evaluated_entity.loader == original_loader
        for rule in evaluated_entity.rules:
            assert rule.loader == original_loader
