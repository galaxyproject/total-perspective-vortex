import os
import time
import tempfile
import shutil
import unittest
from vortex.rules import mapper
from . import mock_galaxy
from galaxy.jobs.mapper import JobMappingException


class TestResourceParserUser(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool, user, datasets, mapping_rules_path=None):
        galaxy_app = mock_galaxy.App()
        job = mock_galaxy.Job()
        for d in datasets:
            job.add_input_dataset(d)
        mapper_config = mapping_rules_path or os.path.join(os.path.dirname(__file__), 'fixtures/mapping-rules.yml')
        mapper.ACTIVE_DESTINATION_MAPPER = None
        return mapper.map_tool_to_destination(galaxy_app, job, tool, user, mapper_config_file=mapper_config)

    def test_map_rule_size_small(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('ford', 'prefect@vortex.org')
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=1))]

        with self.assertRaises(JobMappingException):
            self._map_to_destination(tool, user, datasets)

    def test_map_rule_size_medium(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5))]

        destination = self._map_to_destination(tool, user, datasets)
        self.assertEqual(destination.id, "k8s_environment")
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'TEST_JOB_SLOTS'], ['2'])
        self.assertEqual(destination.params['NATIVE_SPEC'], '--mem 6 --cores 2')

    def test_map_rule_size_large(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=15))]

        destination = self._map_to_destination(tool, user, datasets)
        self.assertIsNone(destination)

    def test_map_rule_user(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('arthur', 'arthur@vortex.org')
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=15))]

        with self.assertRaises(JobMappingException):
            self._map_to_destination(tool, user, datasets)

    def test_map_rule_user_params(self):
        tool = mock_galaxy.Tool('bwa')
        user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5))]

        destination = self._map_to_destination(tool, user, datasets)
        self.assertEqual(destination.id, "k8s_environment")
        self.assertEqual([env['value'] for env in destination.env if env['name'] == 'TEST_JOB_SLOTS_USER'], ['4'])
        self.assertEqual(destination.params['NATIVE_SPEC_USER'], '--mem 16 --cores 4')

    def test_rules_automatically_reload_on_update(self):
        with tempfile.NamedTemporaryFile('w+t') as tmp_file:
            rule_file = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-rules.yml')
            shutil.copy2(rule_file, tmp_file.name)

            tool = mock_galaxy.Tool('bwa')
            user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')
            datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5))]

            destination = self._map_to_destination(tool, user, datasets, mapping_rules_path=tmp_file.name)
            self.assertEqual([env['value'] for env in destination.env if env['name'] == 'TEST_JOB_SLOTS_USER'], ['4'])

            # update the rule file
            updated_rule_file = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-rules-changed.yml')
            shutil.copy2(updated_rule_file, tmp_file.name)

            # wait for reload
            time.sleep(1)

            # should have loaded the new rules
            destination = self._map_to_destination(tool, user, datasets, mapping_rules_path=tmp_file.name)
            self.assertEqual([env['value'] for env in destination.env if env['name'] == 'TEST_JOB_SLOTS_USER'], ['8'])
