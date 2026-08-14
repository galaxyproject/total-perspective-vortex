"""Integration tests for the weighted_choice helper in TPV config files."""

import os
import unittest
from unittest.mock import patch

from galaxy.jobs import JobDestination

from tpv.commands.test import mock_galaxy
from tpv.rules import gateway


class TestMapperWeightedChoice(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool: mock_galaxy.Tool, user: mock_galaxy.User, tpv_config_path: str) -> JobDestination:
        galaxy_app = mock_galaxy.App(job_conf=os.path.join(os.path.dirname(__file__), "fixtures/job_conf.yml"))
        job = mock_galaxy.Job()
        gateway.ACTIVE_DESTINATION_MAPPERS = {}
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, tpv_config_files=[tpv_config_path])  # type: ignore[arg-type]

    def test_weighted_choice_sets_job_working_directory_param(self) -> None:
        """helpers.weighted_choice in a params f-string should set job_working_directory."""
        fixture = os.path.join(
            os.path.dirname(__file__),
            "fixtures/mapping-weighted-choice.yml",
        )
        tool = mock_galaxy.Tool("bwa")
        user = mock_galaxy.User("ford", "prefect@vortex.org")

        # Patch random.choices to always return the first item (weight 3, /fast/jobs)
        pool = [
            {"value": "/fast/jobs", "weight": 3},
            {"value": "/slow/jobs", "weight": 1},
        ]
        with patch("tpv.core.helpers.random.choices", return_value=[pool[0]]):
            destination = self._map_to_destination(tool, user, tpv_config_path=fixture)

        self.assertIn("job_working_directory", destination.params)
        self.assertEqual(destination.params["job_working_directory"], "/fast/jobs")

    def test_weighted_choice_selects_different_path_when_patched(self) -> None:
        """When random.choices returns a different item, the param should reflect it."""
        fixture = os.path.join(
            os.path.dirname(__file__),
            "fixtures/mapping-weighted-choice.yml",
        )
        tool = mock_galaxy.Tool("bwa")
        user = mock_galaxy.User("ford", "prefect@vortex.org")

        pool = [
            {"value": "/fast/jobs", "weight": 3},
            {"value": "/slow/jobs", "weight": 1},
        ]
        # Patch to return the second item
        with patch("tpv.core.helpers.random.choices", return_value=[pool[1]]):
            destination = self._map_to_destination(tool, user, tpv_config_path=fixture)

        self.assertEqual(destination.params["job_working_directory"], "/slow/jobs")
