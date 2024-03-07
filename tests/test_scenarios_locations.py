import os
import time
import tempfile
import pathlib
import responses
import shutil
import unittest
from galaxy.jobs.mapper import JobMappingException
from tpv.rules import gateway
from tpv.commands.test import mock_galaxy
from tpv.core.helpers import get_dataset_attributes

class TestScenarios(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool, user, datasets=[], tpv_config_path=None, job_conf=None, app=None):
        if job_conf:
            galaxy_app = mock_galaxy.App(job_conf=os.path.join(os.path.dirname(__file__), job_conf))
        elif app:
            galaxy_app = app
        else:
            galaxy_app = mock_galaxy.App(job_conf=os.path.join(os.path.dirname(__file__), 'fixtures/job_conf.yml'))
        job = mock_galaxy.Job()
        for d in datasets:
            job.add_input_dataset(d)
        tpv_config = tpv_config_path or os.path.join(os.path.dirname(__file__), 'fixtures/mapping-rules.yml')
        gateway.ACTIVE_DESTINATION_MAPPER = None
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, tpv_config_files=[tpv_config])

    def test_scenario_esg_group_user(self):
        """
        pulsar-hm2-user is a user to specifically run jobs on hm2 with a minimum spec. Regardless of anything else.
        Each endpoint will have a user that does this.
        """

        tool = mock_galaxy.Tool('trinity')
        user = mock_galaxy.User('pulsar-hm2-user', 'pulsar-hm2-user@unimelb.edu.au', roles=["ga_admins"])
        datasets = [mock_galaxy.DatasetAssociation("input", mock_galaxy.Dataset("input.fastq",
                                                                                file_size=1000*1024**3,
                                                                                object_store_id="files1"))]
        rules_file = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-locations.yml')
        destination = self._map_to_destination(tool, user, datasets=datasets, tpv_config_path=rules_file,
                                                job_conf='fixtures/job_conf_scenario_locations.yml')
        self.assertEqual(destination.id, "pulsar_australia")

    def test_scenario_esg_group_user_api(self):
        """
        pulsar-hm2-user is a user to specifically run jobs on hm2 with a minimum spec. Regardless of anything else.
        Each endpoint will have a user that does this.
        """

        def create_job(app, destination):
            sa_session = app.model.context

            u = app.model.User(email="highmemuser@unimelb.edu.au", password="password")
            j = app.model.Job()
            j.user = u
            j.tool_id = "trinity"
            j.state = "running"
            j.destination_id = destination

            sa_session.add(j)
            sa_session.flush()

        def create_job_state_history(app, destination):
            sa_session = app.model.context

            u = app.model.User(email="highmemuser@unimelb.edu.au", password="password")
            j = app.model.Job()
            j.user = u
            j.tool_id = "trinity"
            j.state = state
            j.destination_id = destination

            sa_session.add(j)
            sa_session.flush()

        app = mock_galaxy.App(
            job_conf=os.path.join(os.path.dirname(__file__), 'fixtures/job_conf_scenario_locations.yml'),
            create_model=True)
        create_job(app, "highmem_pulsar_1")
        create_job(app, "highmem_pulsar_2")
        create_job(app, "highmem_pulsar_1")
        create_job(app, "highmem_pulsar_2")

        tool = mock_galaxy.Tool('trinity')
        user = mock_galaxy.User('pulsar-hm2-user', 'pulsar-hm2-user@unimelb.edu.au', roles=["ga_admins"])
        datasets = [mock_galaxy.DatasetAssociation("input", mock_galaxy.Dataset("input.fastq",
                                                                                file_size=1000*1024**3,
                                                                                object_store_id="object_store_australia"))]
        rules_file = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-locations-api.yml')
        destination = self._map_to_destination(tool, user, datasets=datasets, tpv_config_path=rules_file,
                                                job_conf='fixtures/job_conf_scenario_locations.yml', app=app)
        self.assertEqual(destination.id, "pulsar_australia")

