from collections import OrderedDict
import contextlib
import io
import os
import sys
import unittest

import pytest
import yaml
from galaxy.jobs.mapper import JobMappingException

from tpv.core.shell import main


@pytest.fixture
def chdir_tests(monkeypatch):
    monkeypatch.chdir(os.path.dirname(__file__))


def run_python_script(command, args):

    @contextlib.contextmanager
    def redirect_argv(new_argv):
        saved_argv = sys.argv[:]
        sys.argv = new_argv
        yield
        sys.argv = saved_argv

    with contextlib.redirect_stderr(io.StringIO()) as stderr:
        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            with redirect_argv(args):
                command()
                return stderr.getvalue() + stdout.getvalue()


class TPVShellTestCase(unittest.TestCase):

    @staticmethod
    def call_shell_command(*args):
        return run_python_script(main, list(args))

    def test_lint_no_errors_non_verbose(self):
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-usegalaxy-dev.yml')
        output = self.call_shell_command("tpv", "lint", tpv_config)
        self.assertTrue(
            "lint successful" in output,
            f"Expected lint to be successful but output was: {output}")

    def test_lint_no_errors_verbose(self):
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-usegalaxy-dev.yml')
        output = self.call_shell_command("tpv", "-vvvv", "lint", tpv_config)
        self.assertTrue(
            "lint successful" in output,
            f"Expected lint to be successful but output was: {output}")

    def test_lint_syntax_error(self):
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-syntax-error.yml')
        output = self.call_shell_command("tpv", "lint", tpv_config)
        self.assertTrue(
            "lint failed" in output,
            f"Expected lint to fail but output was: {output}")
        self.assertTrue(
            "oops syntax!" in output,
            f"Expected lint to fail but output was: {output}")

    def test_lint_no_runner_defined(self):
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/linter/linter-no-runner-defined.yml')
        output = self.call_shell_command("tpv", "-vv", "lint", tpv_config)
        self.assertTrue(
            "lint failed" in output,
            f"Expected lint to fail but output was: {output}")
        self.assertTrue(
            "Destination 'local'" in output,
            f"Expected absence of runner param to be reported for destination local but output was: {output}")
        self.assertTrue(
            "Destination 'another_env_without_runner'" in output,
            "Expected absence of runner param to be reported for destination another_env_without_runner "
            f"but output was: {output}")
        self.assertTrue(
            "Destination 'k8s_environment'" not in output,
            f"Did not expect 'k8s_environment' to be reported as it defines the runner param but output was: {output}")

    def test_lint_destination_defines_cores_instead_of_accepted_cores(self):
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/linter/linter-legacy-destinations.yml')
        output = self.call_shell_command("tpv", "-vv", "lint", tpv_config)
        self.assertTrue(
            "lint failed" in output,
            f"Expected lint to fail but output was: {output}")
        self.assertTrue(
            "The destination named: local_with_mem" in output,
            f"Expected an errors when cores, mem or gpu are defined on a destination but output was: {output}")
        self.assertTrue(
            "The destination named: k8s_environment_with_cores" in output,
            f"Expected an errors when cores, mem or gpu are defined on a destination but output was: {output}")
        self.assertTrue(
            "The destination named: another_env_with_gpus" in output,
            f"Expected an errors when cores, mem or gpu are defined on a destination but output was: {output}")
        self.assertTrue(
            "working_dest" not in output,
            f"Did not expect destination: `working_dest` to be in the output, but found: {output}")

    def test_warn_if_default_inherits_not_marked_abstract(self):
        tpv_config = os.path.join(os.path.dirname(__file__),
                                  'fixtures/linter/linter-default-inherits-marked-abstract.yml')
        output = self.call_shell_command("tpv", "-vvvv", "lint", tpv_config)
        self.assertTrue(
            "WARNING" in output and "The tool named: default is marked globally as" in output,
            f"Expected a warning when the default abstract class for a tool is not marked abstract but output "
            f"was: {output}")
        self.assertTrue(
            "WARNING" in output and "The destination named: default is marked globally as" in output,
            f"Expected a warning when the default abstract class for a tool is not marked abstract but output "
            f"was: {output}")

    def test_format_basic(self):
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/formatter/formatter-basic.yml')
        with open(tpv_config) as f:
            before_formatting = yaml.safe_load(f)

        output = self.call_shell_command("tpv", "format", tpv_config)
        after_formatting = yaml.safe_load(output)

        self.assertTrue(before_formatting == after_formatting,
                        "Expected content to be the same after formatting")
        self.assertTrue(OrderedDict(before_formatting) != OrderedDict(after_formatting),
                        "Expected ordering to be different after formatting")

        # keys should be in expected order
        self.assertEqual(list(before_formatting.keys()), ["global", "destinations", "users", "tools"])
        self.assertEqual(list(after_formatting.keys()), ["global", "tools", "users", "destinations"])

        # default inherits should be first
        self.assertEqual(list(before_formatting['tools']).index('base_default'), 3)
        self.assertEqual(list(after_formatting['tools']).index('base_default'), 0)
        self.assertEqual(list(before_formatting['destinations']).index('base_default'), 2)
        self.assertEqual(list(after_formatting['destinations']).index('base_default'), 0)

        # scheduling tags should be in expected order
        self.assertEqual(list(before_formatting['tools']['base_default']['scheduling'].keys()),
                         ["prefer", "accept", "reject", "require"])
        self.assertEqual(list(after_formatting['tools']['base_default']['scheduling'].keys()),
                         ["require", "prefer", "accept", "reject"])

        # context var order should not be changed
        self.assertEqual(list(before_formatting['tools']['base_default']['context'].keys()),
                         ['my_context_var1', 'another_context_var2'])
        self.assertEqual(list(before_formatting['tools']['base_default']['context'].keys()),
                         list(after_formatting['tools']['base_default']['context'].keys()))

        # params should be in alphabetical order
        self.assertEqual(list(before_formatting['tools']['base_default']['params'].keys()),
                         ['nativeSpecification', 'anotherParam'])
        self.assertEqual(list(after_formatting['tools']['base_default']['params'].keys()),
                         ['anotherParam', 'nativeSpecification'])

        # env should be in alphabetical order
        self.assertEqual(list(before_formatting['tools']['base_default']['env'].keys()),
                         ['some_env', 'another_env'])
        self.assertEqual(list(after_formatting['tools']['base_default']['env'].keys()),
                         ['another_env', 'some_env'])

    def test_format_rules(self):
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/formatter/formatter-basic.yml')
        with open(tpv_config) as f:
            before_formatting = yaml.safe_load(f)

        output = self.call_shell_command("tpv", "format", tpv_config)
        after_formatting = yaml.safe_load(output)

        # rules should remain in original order
        self.assertEqual(before_formatting['tools']['.*hifiasm.*']['rules'][0]['id'], "my_rule_2")
        self.assertEqual(after_formatting['tools']['.*hifiasm.*']['rules'][0]['id'], "my_rule_2")
        self.assertEqual(before_formatting['tools']['.*hifiasm.*']['rules'][1]['id'], "my_rule_1")
        self.assertEqual(after_formatting['tools']['.*hifiasm.*']['rules'][1]['id'], "my_rule_1")

        # rule elements should be in expected order
        self.assertEqual(list(before_formatting['tools']['.*hifiasm.*']['rules'][0].keys()),
                         ['mem', 'if', 'id', 'cores', 'context', 'params', 'env', 'scheduling'])
        self.assertEqual(list(after_formatting['tools']['.*hifiasm.*']['rules'][0].keys()),
                         ['id', 'if', 'context', 'cores', 'mem', 'env', 'params', 'scheduling'])

        # scheduling tags within rules should be in expected order
        self.assertEqual(list(before_formatting['tools']['.*hifiasm.*']['rules'][0]['scheduling'].keys()),
                         ['accept', 'prefer', 'reject', 'require'])
        self.assertEqual(list(after_formatting['tools']['.*hifiasm.*']['rules'][0]['scheduling'].keys()),
                         ["require", "prefer", "accept", "reject"])

        # context var order should not be changed
        self.assertEqual(list(before_formatting['tools']['.*hifiasm.*']['rules'][0]['context'].keys()),
                         ['myvar', 'anothervar'])
        self.assertEqual(list(before_formatting['tools']['.*hifiasm.*']['rules'][0]['context'].keys()),
                         list(after_formatting['tools']['.*hifiasm.*']['rules'][0]['context'].keys()))

        # params order should not be changed
        self.assertEqual(list(before_formatting['tools']['.*hifiasm.*']['rules'][0]['params'].keys()),
                         ['MY_PARAM2', 'MY_PARAM1'])
        self.assertEqual(list(after_formatting['tools']['.*hifiasm.*']['rules'][0]['params'].keys()),
                         ['MY_PARAM1', 'MY_PARAM2'])

        # env order should not be changed
        self.assertEqual(list(before_formatting['tools']['.*hifiasm.*']['rules'][0]['env'].keys()),
                         ['SOME_ENV2', 'SOME_ENV1'])
        self.assertEqual(list(after_formatting['tools']['.*hifiasm.*']['rules'][0]['env'].keys()),
                         ['SOME_ENV1', 'SOME_ENV2'])

    def test_format_error(self):
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/file-does-not-exist.yml')
        output = self.call_shell_command("tpv", "format", tpv_config)
        self.assertTrue(
            "format failed" in output,
            f"Expected format to fail but output was: {output}")

    def test_format_string_block_handling(self):
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/formatter/formatter-string-types-input.yml')
        output = self.call_shell_command("tpv", "format", tpv_config)
        with open(os.path.join(os.path.dirname(__file__),
                               'fixtures/formatter/formatter-string-types-formatted.yml')) as f:
            expected_output = f.read()
        self.assertEqual(output, expected_output)

    def test_format_lengthy_key_handling(self):
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/formatter/formatter-long-key-input.yml')
        output = self.call_shell_command("tpv", "format", tpv_config)
        with open(os.path.join(os.path.dirname(__file__), 'fixtures/formatter/formatter-long-key-formatted.yml')) as f:
            expected_output = f.read()
        self.assertEqual(output, expected_output)

    def test_format_tool_sort_order(self):
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/formatter/formatter-tool-sort-order-input.yml')
        output = self.call_shell_command("tpv", "format", tpv_config)
        with open(os.path.join(os.path.dirname(__file__),
                               'fixtures/formatter/formatter-tool-sort-order-formatted.yml')) as f:
            expected_output = f.read()
        self.assertEqual(output, expected_output)

    @pytest.mark.usefixtures("chdir_tests")
    def test_dry_run_tpv_config_from_job_conf_default_tool(self):
        job_config = 'fixtures/job_conf_dry_run.yml'
        output = self.call_shell_command("tpv", "dry-run", "--job-conf", job_config)
        self.assertTrue("id: local" in output,
                        f"Expected 'id: local' destination\n{output}")

    @pytest.mark.usefixtures("chdir_tests")
    def test_dry_run_tpv_config_from_job_conf_pulsar_tool(self):
        job_config = 'fixtures/job_conf_dry_run.yml'
        output = self.call_shell_command("tpv", "dry-run", "--job-conf", job_config, "--tool", "bwa")
        self.assertTrue("id: k8s_environment" in output,
                        f"Expected 'id: k8s_environment' destination\n{output}")

    @pytest.mark.usefixtures("chdir_tests")
    def test_dry_run_tpv_config_from_job_conf_unschedulable_tool(self):
        job_config = 'fixtures/job_conf_dry_run.yml'
        with self.assertRaises(JobMappingException):
            self.call_shell_command("tpv", "dry-run", "--job-conf", job_config, "--tool", "unschedulable_tool")

    @pytest.mark.usefixtures("chdir_tests")
    def test_dry_run_tpv_config_from_job_conf_regex_tool(self):
        job_config = 'fixtures/job_conf_dry_run.yml'
        output = self.call_shell_command("tpv", "dry-run", "--job-conf", job_config, "--tool", "regex_tool/hoopy_frood")
        self.assertTrue("id: k8s_environment" in output,
                        f"Expected 'id: k8s_environment' destination\n{output}")

    def test_dry_run_input_size_piddling(self):
        job_config = os.path.join(os.path.dirname(__file__), 'fixtures/job_conf_dry_run.yml')
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-rules.yml')
        with self.assertRaises(JobMappingException):
            self.call_shell_command("tpv", "dry-run", "--job-conf", job_config, tpv_config)

    def test_dry_run_conditional_input_size_ok(self):
        job_config = os.path.join(os.path.dirname(__file__), 'fixtures/job_conf_dry_run.yml')
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-rules.yml')
        output = self.call_shell_command(
            "tpv", "dry-run", "--job-conf", job_config, "--tool", "bwa", "--input-size", "6", tpv_config)
        self.assertTrue("id: k8s_environment" in output,
                        f"Expected 'id: k8s_environment' destination\n{output}")

    def test_dry_run_conditional_input_size_too_big(self):
        job_config = os.path.join(os.path.dirname(__file__), 'fixtures/job_conf_dry_run.yml')
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-rules.yml')
        with self.assertRaises(JobMappingException):
            self.call_shell_command(
                "tpv", "dry-run", "--job-conf", job_config, "--tool", "bwa", "--input-size", "20", tpv_config)

    def test_dry_run_user_email(self):
        job_config = os.path.join(os.path.dirname(__file__), 'fixtures/job_conf_dry_run.yml')
        tpv_config = os.path.join(os.path.dirname(__file__), 'fixtures/mapping-rules.yml')
        output = self.call_shell_command(
            "tpv", "dry-run", "--job-conf", job_config, "--input-size", "6", "--user", "fairycake@vortex.org",
            tpv_config)
        self.assertTrue("name: TEST_JOB_SLOTS" in output,
                        f"Expected 'name: TEST_JOB_SLOTS' in destination\n{output}")
