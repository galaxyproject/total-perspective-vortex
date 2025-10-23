import os
import shutil
import tempfile
import time
import unittest

from galaxy.jobs import JobDestination
from galaxy.jobs.mapper import JobMappingException, JobNotReadyException

from tpv.commands.test import mock_galaxy
from tpv.rules import gateway


class TestMapperRules(unittest.TestCase):

    @staticmethod
    def _map_to_destination(
        tool,
        user,
        datasets,
        param_values=None,
        tpv_config_files=None,
        app=None,
        referrer=None,
        reset_mappers=True,
    ):
        galaxy_app = app or mock_galaxy.App(job_conf=os.path.join(os.path.dirname(__file__), "fixtures/job_conf.yml"))
        job = mock_galaxy.Job()
        for d in datasets:
            job.add_input_dataset(d)
        if param_values:
            job.param_values = param_values
        tpv_configs = tpv_config_files or [os.path.join(os.path.dirname(__file__), "fixtures/mapping-rules.yml")]
        if reset_mappers:
            gateway.ACTIVE_DESTINATION_MAPPERS = {}
        return gateway.map_tool_to_destination(
            galaxy_app, job, tool, user, tpv_config_files=tpv_configs, referrer=JobDestination(id="tpv_dispatcher")
        )

    def test_map_rule_size_small(self):
        tool = mock_galaxy.Tool("bwa")
        user = mock_galaxy.User("ford", "prefect@vortex.org")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=1 * 1024**3))]

        with self.assertRaises(JobMappingException):
            self._map_to_destination(tool, user, datasets)

    def test_map_rule_size_medium(self):
        tool = mock_galaxy.Tool("bwa")
        user = mock_galaxy.User("gargravarr", "fairycake@vortex.org")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5 * 1024**3))]

        destination = self._map_to_destination(tool, user, datasets)
        self.assertEqual(destination.id, "k8s_environment")
        self.assertEqual(
            [env["value"] for env in destination.env if env["name"] == "TEST_JOB_SLOTS"],
            ["4"],
        )
        self.assertEqual(destination.params["native_spec"], "--mem 16 --cores 4")

    def test_map_rule_size_large(self):
        tool = mock_galaxy.Tool("bwa")
        user = mock_galaxy.User("gargravarr", "fairycake@vortex.org")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=15 * 1024**3))]

        with self.assertRaisesRegex(JobMappingException, "No destinations are available to fulfill request"):
            self._map_to_destination(tool, user, datasets)

    def test_map_rule_user(self):
        tool = mock_galaxy.Tool("bwa")
        user = mock_galaxy.User("arthur", "arthur@vortex.org")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=15 * 1024**3))]

        with self.assertRaises(JobMappingException):
            self._map_to_destination(tool, user, datasets)

    def test_map_rule_user_params(self):
        tool = mock_galaxy.Tool("bwa")
        user = mock_galaxy.User("gargravarr", "fairycake@vortex.org")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5 * 1024**3))]

        destination = self._map_to_destination(tool, user, datasets)
        self.assertEqual(destination.id, "k8s_environment")
        self.assertEqual(
            [env["value"] for env in destination.env if env["name"] == "TEST_JOB_SLOTS_USER"],
            ["4"],
        )
        self.assertEqual(destination.params["native_spec_user"], "--mem 16 --cores 4")

    def test_rules_automatically_reload_on_update(self):
        with tempfile.NamedTemporaryFile("w+t") as tmp_file:
            rule_file = os.path.join(os.path.dirname(__file__), "fixtures/mapping-rules.yml")
            shutil.copy2(rule_file, tmp_file.name)

            tool = mock_galaxy.Tool("bwa")
            user = mock_galaxy.User("gargravarr", "fairycake@vortex.org")
            datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5 * 1024**3))]

            destination = self._map_to_destination(
                tool,
                user,
                datasets,
                tpv_config_files=[tmp_file.name],
                reset_mappers=True,
            )
            self.assertEqual(
                [env["value"] for env in destination.env if env["name"] == "TEST_JOB_SLOTS_USER"],
                ["4"],
            )

            # update the rule file
            updated_rule_file = os.path.join(os.path.dirname(__file__), "fixtures/mapping-rules-changed.yml")
            shutil.copy2(updated_rule_file, tmp_file.name)

            # wait for reload
            time.sleep(2)

            # should have loaded the new rules
            destination = self._map_to_destination(
                tool,
                user,
                datasets,
                tpv_config_files=[tmp_file.name],
                reset_mappers=False,
            )
            self.assertEqual(
                [env["value"] for env in destination.env if env["name"] == "TEST_JOB_SLOTS_USER"],
                ["8"],
            )

    def test_multiple_files_automatically_reload_on_update(self):
        with tempfile.NamedTemporaryFile("w+t") as tmp_file1, tempfile.NamedTemporaryFile("w+t") as tmp_file2:
            rule_file = os.path.join(os.path.dirname(__file__), "fixtures/mapping-rules.yml")
            shutil.copy2(rule_file, tmp_file1.name)
            rule_file = os.path.join(os.path.dirname(__file__), "fixtures/mapping-rules-extra.yml")
            shutil.copy2(rule_file, tmp_file2.name)

            tool = mock_galaxy.Tool("bwa")
            user = mock_galaxy.User("gargravarr", "fairycake@vortex.org")
            datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5 * 1024**3))]

            destination = self._map_to_destination(
                tool,
                user,
                datasets,
                tpv_config_files=[tmp_file1.name, tmp_file2.name],
                reset_mappers=True,
            )
            self.assertEqual(
                [env["value"] for env in destination.env if env["name"] == "TEST_JOB_SLOTS_USER"],
                ["3"],
            )

            # update the rule files
            updated_rule_file = os.path.join(os.path.dirname(__file__), "fixtures/mapping-rules-changed.yml")
            shutil.copy2(updated_rule_file, tmp_file1.name)
            updated_rule_file = os.path.join(os.path.dirname(__file__), "fixtures/mapping-rules-changed-extra.yml")
            shutil.copy2(updated_rule_file, tmp_file2.name)

            # wait for reload
            time.sleep(2)

            # should have loaded the new rules
            destination = self._map_to_destination(
                tool,
                user,
                datasets,
                tpv_config_files=[tmp_file1.name, tmp_file2.name],
                reset_mappers=False,
            )
            self.assertEqual(
                [env["value"] for env in destination.env if env["name"] == "TEST_JOB_SLOTS_USER"],
                ["10"],
            )

    def test_rules_automatically_reload_when_multi_referrer(self):
        with tempfile.NamedTemporaryFile("w+t") as tmp_file:
            rule_file = os.path.join(os.path.dirname(__file__), "fixtures/mapping-rules.yml")
            shutil.copy2(rule_file, tmp_file.name)

            tool = mock_galaxy.Tool("bwa")
            user = mock_galaxy.User("gargravarr", "fairycake@vortex.org")
            datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5 * 1024**3))]

            destination = self._map_to_destination(
                tool,
                user,
                datasets,
                tpv_config_files=[tmp_file.name],
                referrer=JobDestination(id="tpv_dispatcher1"),
                reset_mappers=True,
            )
            self.assertEqual(
                [env["value"] for env in destination.env if env["name"] == "TEST_JOB_SLOTS_USER"],
                ["4"],
            )
            destination = self._map_to_destination(
                tool,
                user,
                datasets,
                tpv_config_files=[tmp_file.name],
                referrer=JobDestination(id="tpv_dispatcher2"),
                reset_mappers=False,
            )
            self.assertEqual(
                [env["value"] for env in destination.env if env["name"] == "TEST_JOB_SLOTS_USER"],
                ["4"],
            )

            # update the rule file
            updated_rule_file = os.path.join(os.path.dirname(__file__), "fixtures/mapping-rules-changed.yml")
            shutil.copy2(updated_rule_file, tmp_file.name)

            # wait for reload
            time.sleep(2)

            # should have loaded the new rules for both referrers
            destination = self._map_to_destination(
                tool,
                user,
                datasets,
                tpv_config_files=[tmp_file.name],
                referrer=JobDestination(id="tpv_dispatcher1"),
                reset_mappers=False,
            )
            self.assertEqual(
                [env["value"] for env in destination.env if env["name"] == "TEST_JOB_SLOTS_USER"],
                ["8"],
            )
            destination = self._map_to_destination(
                tool,
                user,
                datasets,
                tpv_config_files=[tmp_file.name],
                referrer=JobDestination(id="tpv_dispatcher2"),
                reset_mappers=False,
            )
            self.assertEqual(
                [env["value"] for env in destination.env if env["name"] == "TEST_JOB_SLOTS_USER"],
                ["8"],
            )

    def test_map_with_syntax_error(self):
        tool = mock_galaxy.Tool("bwa")
        user = mock_galaxy.User("ford", "prefect@vortex.org")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=1 * 1024**3))]

        with self.assertRaises(SyntaxError):
            tpv_config = os.path.join(os.path.dirname(__file__), "fixtures/mapping-syntax-error.yml")
            self._map_to_destination(tool, user, datasets, tpv_config_files=[tpv_config])

    def test_map_with_execute_block(self):
        tool = mock_galaxy.Tool("bwa")
        user = mock_galaxy.User("ford", "prefect@vortex.org")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=7 * 1024**3))]

        with self.assertRaises(JobNotReadyException):
            tpv_config = os.path.join(os.path.dirname(__file__), "fixtures/mapping-rule-execute.yml")
            self._map_to_destination(tool, user, datasets, tpv_config_files=[tpv_config])

    def test_map_with_execute_block_side_effects(self):
        tool = mock_galaxy.Tool("bwa")
        user = mock_galaxy.User("ford", "prefect@vortex.org")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=11 * 1024**3))]

        tpv_config = os.path.join(os.path.dirname(__file__), "fixtures/mapping-rule-execute.yml")
        destination = self._map_to_destination(tool, user, datasets, tpv_config_files=[tpv_config])
        self.assertEqual(destination.params["my_brand_new_param"], "hello_world")
        self.assertEqual(destination.params["native_spec"], "--mem 24 --cores 8")

    def test_job_args_match_helper(self):
        tool = mock_galaxy.Tool("limbo")
        user = mock_galaxy.User("gag", "gaghalfrunt@vortex.org")
        tpv_config = os.path.join(os.path.dirname(__file__), "fixtures/mapping-rule-argument-based.yml")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=7 * 1024**3))]
        param_values = {
            "output_style": "flat",
            "colour": {"nighttime": "blue"},
            "input_opts": {"tabs_to_spaces": False, "db_selector": "db"},
        }
        destination = self._map_to_destination(tool, user, datasets, param_values, tpv_config_files=[tpv_config])
        self.assertEqual(destination.id, "k8s_environment")

    def test_concurrent_job_count_helper(self):

        def create_user(app, mock_user):
            sa_session = app.model.context
            user = app.model.User(
                username=mock_user.username,
                email=mock_user.email,
                password="helloworld",
            )
            sa_session.add(user)
            sa_session.flush()
            return user.id

        def create_job(app, mock_user, mock_tool):
            sa_session = app.model.context

            # get user
            query = app.model.context.query(app.model.User)
            query = query.filter(app.model.User.table.c.email == mock_user.email)
            user = query.first()

            job = app.model.Job()
            job.user = user
            job.tool_id = mock_tool.id
            job.state = "running"
            job.destination_id = "local"
            sa_session.add(job)
            sa_session.flush()
            return job.id

        tool_user_limit_2 = mock_galaxy.Tool("toolshed.g2.bx.psu.edu/repos/rnateam/mafft/rbc_mafft/7.221.3")
        tool_total_limit_3 = mock_galaxy.Tool("toolshed.g2.bx.psu.edu/repos/artbio/repenrich/repenrich/1.6.1")
        user_eccentrica = mock_galaxy.User("eccentrica", "eccentricagallumbits@vortex.org")
        user_roosta = mock_galaxy.User("roosta", "roosta@vortex.org")

        app = mock_galaxy.App(
            job_conf=os.path.join(os.path.dirname(__file__), "fixtures/job_conf.yml"),
            create_model=True,
        )

        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=7 * 1024**3))]
        tpv_config = os.path.join(os.path.dirname(__file__), "fixtures/mapping-rule-tool-limits.yml")

        user_roosta.id = create_user(app, user_roosta)
        user_eccentrica.id = create_user(app, user_eccentrica)

        # set 2 jobs rbc_mafft jobs running for user roosta
        create_job(app, user_roosta, tool_user_limit_2)
        create_job(app, user_roosta, tool_user_limit_2)

        # roosta cannot create another rbc_mafft job
        with self.assertRaises(JobNotReadyException):
            tpv_config = os.path.join(os.path.dirname(__file__), "fixtures/mapping-rule-tool-limits.yml")
            self._map_to_destination(
                tool_user_limit_2,
                user_roosta,
                datasets,
                tpv_config_files=[tpv_config],
                app=app,
            )

        # eccentrica can run a rbc_mafft job
        destination = self._map_to_destination(
            tool_user_limit_2,
            user_eccentrica,
            datasets,
            tpv_config_files=[tpv_config],
            app=app,
        )
        self.assertEqual(destination.id, "local")

        # set up 3 running jobs for repenrich tool
        create_job(app, user_eccentrica, tool_total_limit_3)
        create_job(app, user_eccentrica, tool_total_limit_3)
        create_job(app, user_eccentrica, tool_total_limit_3)

        # roosta cannot create another repenrich job
        with self.assertRaises(JobNotReadyException):
            tpv_config = os.path.join(os.path.dirname(__file__), "fixtures/mapping-rule-tool-limits.yml")
            self._map_to_destination(
                tool_total_limit_3,
                user_roosta,
                datasets,
                tpv_config_files=[tpv_config],
                app=app,
            )

    def test_tool_version_comparison_helpers(self):
        tpv_config = os.path.join(os.path.dirname(__file__), "fixtures/mapping-rule-tool-limits.yml")
        user = mock_galaxy.User("ford", "prefect@vortex.org")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=1 * 1024**3))]

        def mock_trinity_with_version(version):
            return mock_galaxy.Tool(
                id=f"toolshed.g2.bx.psu.edu/repos/iuc/trinity/trinity/{version}",
                version=version,
            )

        env_keys = [  # env keys that may be added by cooked trinity rules
            "version_gte_2.15.1+galaxy0",
            "version_gt_2.15.1+galaxy0",
            "version_lt_2.10.1+galaxy7",
            "version_lte_2.10.1+galaxy7",
        ]
        # trinity version 3.15.1+galaxy0
        tool = mock_trinity_with_version("3.15.1+galaxy0")
        destination = self._map_to_destination(tool, user, datasets, tpv_config_files=[tpv_config])
        self.assertCountEqual(
            [e.get("name") for e in destination.env if e.get("name") in env_keys],
            ["version_gte_2.15.1+galaxy0", "version_gt_2.15.1+galaxy0"],
        )
        # trinity version 2.15.1+galaxy0
        tool = mock_trinity_with_version("2.15.1+galaxy0")
        destination = self._map_to_destination(tool, user, datasets, tpv_config_files=[tpv_config])
        self.assertCountEqual(
            [e.get("name") for e in destination.env if e.get("name") in env_keys],
            ["version_gte_2.15.1+galaxy0"],
        )
        # trinity version 2.10.1+galaxy6
        tool = mock_trinity_with_version("2.10.1+galaxy6")
        destination = self._map_to_destination(tool, user, datasets, tpv_config_files=[tpv_config])
        self.assertCountEqual(
            [e.get("name") for e in destination.env if e.get("name") in env_keys],
            ["version_lt_2.10.1+galaxy7", "version_lte_2.10.1+galaxy7"],
        )
