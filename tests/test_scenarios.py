import os
import time
import tempfile
import pathlib
import responses
import shutil
import unittest
from galaxy.jobs.mapper import JobMappingException
from vortex.rules import gateway
from . import mock_galaxy


class TestScenarios(unittest.TestCase):

    @staticmethod
    def _map_to_destination(tool, user, datasets=[], vortex_config_path=None, job_conf=None, app=None):
        if job_conf:
            galaxy_app = mock_galaxy.App(job_conf=job_conf)
        elif app:
            galaxy_app = app
        else:
            galaxy_app = mock_galaxy.App()
        job = mock_galaxy.Job()
        for d in datasets:
            job.add_input_dataset(d)
        vortex_config = vortex_config_path or os.path.join(os.path.dirname(__file__), 'fixtures/mapping-rules.yml')
        gateway.ACTIVE_DESTINATION_MAPPER = None
        return gateway.map_tool_to_destination(galaxy_app, job, tool, user, vortex_config_files=[vortex_config])

    def test_scenario_node_marked_offline(self):
        with tempfile.NamedTemporaryFile('w+t') as tmp_file:
            rule_file = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-node-online.yml')
            shutil.copy2(rule_file, tmp_file.name)

            tool = mock_galaxy.Tool('bwa')
            user = mock_galaxy.User('gargravarr', 'fairycake@vortex.org')

            destination = self._map_to_destination(tool, user, vortex_config_path=tmp_file.name)
            self.assertEqual(destination.id, "k8s_environment")

            # update the rule file with one node marked offline
            updated_rule_file = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-node-offline.yml')
            shutil.copy2(updated_rule_file, tmp_file.name)

            # wait for reload
            time.sleep(0.5)

            # should now map to the available node
            destination = self._map_to_destination(tool, user, vortex_config_path=tmp_file.name)
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
        datasets = [mock_galaxy.DatasetAssociation("input", mock_galaxy.Dataset("input.fastq", file_size=10*1024**3))]
        rules_file = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-job-too-small-for-highmem.yml')
        destination = self._map_to_destination(tool, user, datasets=datasets, vortex_config_path=rules_file,
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
        datasets = [mock_galaxy.DatasetAssociation("input", mock_galaxy.Dataset("input.fastq",
                                                                                file_size=0.1*1024**3))]
        rules_file = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-node-offline-high-cpu.yml')
        destination = self._map_to_destination(tool, user, datasets=datasets, vortex_config_path=rules_file,
                                               job_conf='fixtures/job_conf_scenario_usegalaxy_au.yml')
        self.assertEqual(destination.id, "general_pulsar_2")

    @responses.activate
    def test_scenario_trinity_with_rules(self):
        """
        Job gets scheduled to hm2 as high mem and it has lowest utilisation
        """
        responses.add(
            method=responses.GET,
            url="http://stats.genome.edu.au:8086/query",
            body=pathlib.Path(
                os.path.join(os.path.dirname(__file__), 'fixtures/response-trinity-job-with-rules.yml')).read_text(),
            match_querystring=False,
        )

        tool = mock_galaxy.Tool('trinity')
        user = mock_galaxy.User('someone', 'someone@unimelb.edu.au')
        datasets = [mock_galaxy.DatasetAssociation("input", mock_galaxy.Dataset("input.fastq",
                                                                                file_size=0.1*1024**3))]
        rules_file = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-trinity-job-with-rules.yml')
        destination = self._map_to_destination(tool, user, datasets=datasets, vortex_config_path=rules_file,
                                               job_conf='fixtures/job_conf_scenario_usegalaxy_au.yml')
        self.assertEqual(destination.id, "highmem_pulsar_2")

    @responses.activate
    def test_scenario_trinity_job_too_much_data(self):
        """
        Contextual fail message sent to user with reasons that there is too much data. (i.e. that 1TB is > 200GB)
        """
        responses.add(
            method=responses.GET,
            url="http://stats.genome.edu.au:8086/query",
            body=pathlib.Path(
                os.path.join(os.path.dirname(__file__), 'fixtures/response-trinity-job-with-rules.yml')).read_text(),
            match_querystring=False,
        )

        tool = mock_galaxy.Tool('trinity')
        user = mock_galaxy.User('someone', 'someone@unimelb.edu.au')
        datasets = [mock_galaxy.DatasetAssociation("input", mock_galaxy.Dataset("input.fastq",
                                                                                file_size=1000*1024**3))]
        rules_file = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-trinity-job-too-much-data.yml')
        with self.assertRaisesRegex(JobMappingException,
                                    "Input file size of 1000.0GB is > maximum allowed 200GB limit"):
            self._map_to_destination(tool, user, datasets=datasets, vortex_config_path=rules_file,
                                     job_conf='fixtures/job_conf_scenario_usegalaxy_au.yml')

    @responses.activate
    def test_scenario_non_pulsar_enabled_job(self):
        """
        Jobs are scheduled to Slurm even though gps are at lower utilisation as fastp is not pulsar enabled
        """
        responses.add(
            method=responses.GET,
            url="http://stats.genome.edu.au:8086/query",
            body=pathlib.Path(
                os.path.join(os.path.dirname(__file__), 'fixtures/response-non-pulsar-enabled-job.yml')).read_text(),
            match_querystring=False,
        )

        tool = mock_galaxy.Tool('fastp')
        user = mock_galaxy.User('kate', 'kate@unimelb.edu.au')
        datasets = [mock_galaxy.DatasetAssociation("input", mock_galaxy.Dataset("input.fastq",
                                                                                file_size=1000*1024**3))]
        rules_file = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-non-pulsar-enabled-job.yml')
        destination = self._map_to_destination(tool, user, datasets=datasets, vortex_config_path=rules_file,
                                               job_conf='fixtures/job_conf_scenario_usegalaxy_au.yml')
        self.assertEqual(destination.id, "slurm")

    @responses.activate
    def test_scenario_jenkins_bot_user(self):
        """
        jenkins' jobs will run on slurm with minimal core/mem specs
        """
        responses.add(
            method=responses.GET,
            url="http://stats.genome.edu.au:8086/query",
            body=pathlib.Path(
                os.path.join(os.path.dirname(__file__), 'fixtures/response-trinity-job-with-rules.yml')).read_text(),
            match_querystring=False,
        )

        tool = mock_galaxy.Tool('fastp')
        user = mock_galaxy.User('jenkinsbot', 'jenkinsbot@unimelb.edu.au')
        datasets = [mock_galaxy.DatasetAssociation("input", mock_galaxy.Dataset("input.fastq",
                                                                                file_size=1000*1024**3))]
        rules_file = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-jenkins-bot-user.yml')
        destination = self._map_to_destination(tool, user, datasets=datasets, vortex_config_path=rules_file,
                                               job_conf='fixtures/job_conf_scenario_usegalaxy_au.yml')
        self.assertEqual(destination.id, "slurm")

    @responses.activate
    def test_scenario_admin_group_user(self):
        """
        pulsar-hm2-user is a user to specifically run jobs on hm2 with a minimum spec. Regardless of anything else.
        Each endpoint will have a user that does this.
        """
        responses.add(
            method=responses.GET,
            url="http://stats.genome.edu.au:8086/query",
            body=pathlib.Path(
                os.path.join(os.path.dirname(__file__), 'fixtures/response-admin-group-user.yml')).read_text(),
            match_querystring=False,
        )

        tool = mock_galaxy.Tool('trinity')
        user = mock_galaxy.User('pulsar-hm2-user', 'pulsar-hm2-user@unimelb.edu.au', roles=["ga_admins"])
        datasets = [mock_galaxy.DatasetAssociation("input", mock_galaxy.Dataset("input.fastq",
                                                                                file_size=1000*1024**3))]
        rules_file = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-admin-group-user.yml')
        destination = self._map_to_destination(tool, user, datasets=datasets, vortex_config_path=rules_file,
                                               job_conf='fixtures/job_conf_scenario_usegalaxy_au.yml')
        self.assertEqual(destination.id, "highmem_pulsar_2")

    @responses.activate
    def test_scenario_too_many_highmem_jobs(self):
        """
        User can only have up to X high-mem jobs scheduled at one time
        """
        responses.add(
            method=responses.GET,
            url="http://stats.genome.edu.au:8086/query",
            body=pathlib.Path(
                os.path.join(os.path.dirname(__file__), 'fixtures/response-admin-group-user.yml')).read_text(),
            match_querystring=False,
        )

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

        app = mock_galaxy.App(job_conf='fixtures/job_conf_scenario_usegalaxy_au.yml', create_model=True)
        create_job(app, "highmem_pulsar_1")
        create_job(app, "highmem_pulsar_2")
        create_job(app, "highmem_pulsar_1")
        create_job(app, "highmem_pulsar_2")

        tool = mock_galaxy.Tool('trinity')
        user = mock_galaxy.User('highmemuser', 'highmemuser@unimelb.edu.au', roles=["ga_admins"])
        datasets = [mock_galaxy.DatasetAssociation("input", mock_galaxy.Dataset("input.fastq",
                                                                                file_size=1000*1024**3))]
        rules_file = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-too-many-highmem-jobs.yml')
        destination = self._map_to_destination(tool, user, datasets=datasets, vortex_config_path=rules_file, app=app)
        self.assertEqual(destination.id, "highmem_pulsar_1")

        # exceed the limit
        create_job(app, "highmem_pulsar_1")

        with self.assertRaisesRegex(
                JobMappingException, "You cannot have more than 4 high-mem jobs running concurrently"):
            self._map_to_destination(tool, user, datasets=datasets, vortex_config_path=rules_file, app=app)

    @responses.activate
    def test_scenario_usegalaxy_dev(self):
        """
        Check whether usegalaxy.au dev dispatch works
        """
        tool = mock_galaxy.Tool('upload1')
        user = mock_galaxy.User('catherine', 'catherine@unimelb.edu.au')
        datasets = [mock_galaxy.DatasetAssociation("input", mock_galaxy.Dataset("input.fastq",
                                                                                file_size=1000*1024**3))]
        rules_file = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-usegalaxy-dev.yml')
        destination = self._map_to_destination(tool, user, datasets=datasets, vortex_config_path=rules_file,
                                               job_conf='fixtures/job_conf_scenario_usegalaxy_au.yml')
        self.assertEqual(destination.id, "slurm")

        tool = mock_galaxy.Tool('hifiasm')
        destination = self._map_to_destination(tool, user, datasets=datasets, vortex_config_path=rules_file,
                                               job_conf='fixtures/job_conf_scenario_usegalaxy_au.yml')
        self.assertEqual(destination.id, "pulsar-nci-test")
