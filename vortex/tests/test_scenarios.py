import os
import time
import tempfile
import pathlib
import responses
import shutil
import unittest
from vortex.rules import gateway
from . import mock_galaxy


class TestScenarios(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool, user, datasets=[], mapping_rules_path=None, job_conf=None):
        if job_conf:
            galaxy_app = mock_galaxy.App(job_conf=job_conf)
        else:
            galaxy_app = mock_galaxy.App()
        job = mock_galaxy.Job()
        for d in datasets:
            job.add_input_dataset(d)
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

    @responses.activate
    def test_scenario_job_too_small_for_high_memory_node(self):
        """
        Job gets scheduled to gp2 as not enough data for high memory pulsar.
        :return:
        """
        responses.add(
            method=responses.GET,
            url="http://stats.genome.edu.au:8086/query",
            body=pathlib.Path(
                os.path.join(os.path.dirname(__file__), 'fixtures/response-job-too-small-for-highmem.yml')).read_text(),
            match_querystring=False,
        )

        tool = mock_galaxy.Tool('bwa-mem')
        user = mock_galaxy.User('simon', 'simon@unimelb.edu.au')
        datasets = [mock_galaxy.DatasetAssociation("input", mock_galaxy.Dataset("input.fastq", file_size=10))]
        rules_file = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-job-too-small-for-highmem.yml')
        destination = self._map_to_destination(tool, user, datasets=datasets, mapping_rules_path=rules_file,
                                               job_conf='fixtures/job_conf_scenario_usegalaxy_au.yml')
        self.assertEqual(destination.id, "general_pulsar_2")

    @responses.activate
    def test_scenario_node_offline_high_cpu(self):
        """
        Marking destination as offline will prevent jobs from being scheduled there. So this job will be scheduled
        to gp2 even though it has high utilisation as gp1 is offline
        """
        responses.add(
            method=responses.GET,
            url="http://stats.genome.edu.au:8086/query",
            body=pathlib.Path(
                os.path.join(os.path.dirname(__file__), 'fixtures/response-node-offline-high-cpu.yml')).read_text(),
            match_querystring=False,
        )

        tool = mock_galaxy.Tool('bwa-mem')
        user = mock_galaxy.User('steve', 'steve@unimelb.edu.au')
        datasets = [mock_galaxy.DatasetAssociation("input", mock_galaxy.Dataset("input.fastq", file_size=0.1))]
        rules_file = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-node-offline-high-cpu.yml')
        destination = self._map_to_destination(tool, user, datasets=datasets, mapping_rules_path=rules_file,
                                               job_conf='fixtures/job_conf_scenario_usegalaxy_au.yml')
        self.assertEqual(destination.id, "general_pulsar_2")
