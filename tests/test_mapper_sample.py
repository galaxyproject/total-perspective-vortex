import os
import unittest

import pytest
from parameterized import parameterized

from tpv.commands.test import mock_galaxy
from tpv.rules import gateway


class TestMapperSample(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool, job_conf):
        galaxy_app = mock_galaxy.App(
            job_conf=os.path.join(os.path.dirname(__file__), job_conf),
            create_model=True,
        )
        job = mock_galaxy.Job()
        user = mock_galaxy.User("gargravarr", "fairycake@vortex.org")
        tpv_config = os.path.join(os.path.dirname(__file__), "fixtures/mapping-sample.yml")
        gateway.ACTIVE_DESTINATION_MAPPERS = {}
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, tpv_config_files=[tpv_config])

    @parameterized.expand(
        [
            "fixtures/job_conf.yml",
            "fixtures/job_conf.xml",
        ]
    )
    def test_map_sample_tool(self, job_conf):
        tool = mock_galaxy.Tool("sometool")
        destination = self._map_to_destination(tool, job_conf)
        self.assertEqual(destination.id, "local")
        self.assertEqual(destination.params["local_slots"], "2")
