import contextlib
import io
import os
import sys
import unittest

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
