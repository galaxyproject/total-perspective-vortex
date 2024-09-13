import os
import unittest
from tpv.rules import gateway
from tpv.core.entities import Destination
from tpv.core.entities import Tag
from tpv.core.entities import TagType
from tpv.core.entities import Tool
from tpv.core.loader import TPVConfigLoader
from tpv.commands.test import mock_galaxy


class TestEntity(unittest.TestCase):

    @staticmethod
    def _map_to_destination(app, job, tool, user):
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-rule-argument-based.yml')
        gateway.ACTIVE_DESTINATION_MAPPER = None
        return gateway.map_tool_to_destination(app, job, tool, user, tpv_config_files=[tpv_config])

    # issue: https://github.com/galaxyproject/total-perspective-vortex/issues/53
    def test_all_entities_refer_to_same_loader(self):
        app = mock_galaxy.App(job_conf=os.path.join(os.path.dirname(__file__), 'fixtures/job_conf.yml'))
        job = mock_galaxy.Job()

        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')

        # just map something so the ACTIVE_DESTINATION_MAPPER is populated
        self._map_to_destination(app, job, tool, user)

        # get the original loader
        original_loader = gateway.ACTIVE_DESTINATION_MAPPER.loader

        context = {
            'app': app,
            'job': job
        }
        # make sure we are still referring to the same loader after evaluation
        evaluated_entity = gateway.ACTIVE_DESTINATION_MAPPER.match_combine_evaluate_entities(context, tool, user)
        assert evaluated_entity.evaluator == original_loader
        for rule in evaluated_entity.rules:
            assert rule.evaluator == original_loader

    def test_destination_to_dict(self):
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-rule-argument-based.yml')
        loader = TPVConfigLoader.from_url_or_path(tpv_config)

        # create a destination
        destination = loader.config.destinations["k8s_environment"]
        # serialize the destination
        serialized_destination = destination.dict()
        # deserialize the same destination
        deserialized_destination = Destination(evaluator=loader, **serialized_destination)
        # make sure the deserialized destination is the same as the original
        self.assertEqual(deserialized_destination, destination)

    def test_tool_to_dict(self):
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-rule-argument-based.yml')
        loader = TPVConfigLoader.from_url_or_path(tpv_config)

        # create a tool
        tool = loader.config.tools["limbo"]
        # serialize the tool
        serialized_tool = tool.dict()
        # deserialize the same tool
        deserialized_tool = Tool(evaluator=loader, **serialized_tool)
        # make sure the deserialized tool is the same as the original
        self.assertEqual(deserialized_tool, tool)

    def test_tag_equivalence(self):
        tag1 = Tag(value="tag_value", tag_type=TagType.REQUIRE)
        tag2 = Tag(value="tag_value", tag_type=TagType.REQUIRE)
        tag3 = Tag(value="tag_value1", tag_type=TagType.REQUIRE)
        tag4 = Tag(value="tag_value1", tag_type=TagType.PREFER)
        same_as_tag1 = Tag(value="tag_value", tag_type=TagType.REQUIRE)

        self.assertEqual(tag1, tag1)
        self.assertEqual(tag1, same_as_tag1)
        self.assertNotEqual(tag1, tag3)
        self.assertNotEqual(tag1, tag4)
        self.assertNotEqual(tag1, "hello")
