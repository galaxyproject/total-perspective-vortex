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

        # nested keys should be in expected order
        self.assertEqual(list(before_formatting['tools']['base_default']['scheduling'].keys()),
                         ["prefer", "accept", "reject", "require"])
        self.assertEqual(list(after_formatting['tools']['base_default']['scheduling'].keys()),
                         ["require", "prefer", "accept", "reject"])

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
