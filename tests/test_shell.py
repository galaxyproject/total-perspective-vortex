from collections import OrderedDict
import contextlib
import io
import os
import sys
import unittest
import yaml

from tpv.core.shell import main


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

        # rules should be in expected order
        self.assertEqual(list(before_formatting['tools']['.*hifiasm.*']['rules'][0].keys()),
                         ['mem', 'if', 'cores', 'context', 'params', 'env', 'scheduling'])
        self.assertEqual(list(after_formatting['tools']['.*hifiasm.*']['rules'][0].keys()),
                         ['if', 'context', 'cores', 'mem', 'env', 'params', 'scheduling'])

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
        with open(os.path.join(os.path.dirname(__file__), 'fixtures/formatter/formatter-string-types-formatted.yml')) as f:
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
        with open(os.path.join(os.path.dirname(__file__), 'fixtures/formatter/formatter-tool-sort-order-formatted.yml')) as f:
            expected_output = f.read()
        self.assertEqual(output, expected_output)
