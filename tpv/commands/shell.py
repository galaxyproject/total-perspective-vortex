import argparse
import logging
import sys

from ruamel.yaml import YAML, RoundTripRepresenter

from .dryrunner import TPVDryRunner
from .formatter import TPVConfigFormatter
from .linter import TPVConfigLinter
from .linter import TPVLintError

log = logging.getLogger(__name__)


# https://stackoverflow.com/a/64933809
def repr_str(dumper: RoundTripRepresenter, data: str):
    if '\n' in data:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)


# https://stackoverflow.com/a/37445121
def repr_none(dumper: RoundTripRepresenter, data):
    return dumper.represent_scalar(u'tag:yaml.org,2002:null', '')


def tpv_lint_config_file(args):
    try:
        tpv_linter = TPVConfigLinter.from_url_or_path(args.config)
        tpv_linter.lint()
        log.info("lint successful.")
        return 0
    except TPVLintError:
        log.info("lint failed.")
        return 1


def tpv_format_config_file(args):
    try:
        formatter = TPVConfigFormatter.from_url_or_path(args.config)
        yaml = YAML(typ='unsafe', pure=True)
        yaml.Representer = RoundTripRepresenter
        yaml.Representer.add_representer(str, repr_str)
        yaml.Representer.add_representer(type(None), repr_none)
        yaml.default_flow_style = False
        yaml.Emitter.MAX_SIMPLE_KEY_LENGTH = 1024
        yaml.dump(formatter.format(), sys.stdout)
        return 0
    except Exception:
        log.exception("format failed.")
        return 1


def tpv_dry_run_config_files(args):
    dry_runner = TPVDryRunner.from_params(user=args.user, tool=args.tool, job_conf=args.job_conf, tpv_confs=args.config,
                                          input_size=args.input_size)
    destination = dry_runner.run()
    yaml = YAML(typ='unsafe', pure=True)
    yaml.dump(destination, sys.stdout)


def create_parser():
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=lambda args: parser.print_help())

    # debugging and logging settings
    parser.add_argument("-v", "--verbose", action="count",
                        dest="verbosity_count", default=0,
                        help="increases log verbosity for each occurrence.")
    subparsers = parser.add_subparsers(metavar='<subcommand>')

    # File copy commands
    lint_parser = subparsers.add_parser(
        'lint',
        help='loads a TPV configuration file and checks it for syntax errors',
        description="The linter will check yaml syntax and compile python code blocks")
    lint_parser.add_argument(
        'config', type=str,
        help="Path to the TPV config file to lint. Can be a local path or http url.")
    lint_parser.set_defaults(func=tpv_lint_config_file)

    format_parser = subparsers.add_parser(
        'format',
        help='Reformats a TPV configuration file and prints it to stdout.',
        description="The formatter will reorder tools, users etc by name, moving defaults first")
    format_parser.add_argument(
        'config', type=str,
        help="Path to the TPV config file to format. Can be a local path or http url.")
    format_parser.set_defaults(func=tpv_format_config_file)

    dry_run_parser = subparsers.add_parser(
        'dry-run',
        help="Perform a dry run test of a TPV configuration.",
        description="")
    dry_run_parser.add_argument(
        '--job-conf', type=str,
        required=True,
        help="Galaxy job configuration file")
    dry_run_parser.add_argument(
        '--input-size', type=int,
        help="Input dataset size (in GB)")
    dry_run_parser.add_argument(
        '--tool', type=str,
        default='_default_',
        help="Test mapping for Galaxy tool with given ID")
    dry_run_parser.add_argument(
        '--user', type=str,
        help="Test mapping for Galaxy user with username or email")
    dry_run_parser.add_argument(
        'config',
        nargs='*',
        help="TPV configuration files, overrides tpv_config_files in Galaxy job configuration if provided")
    dry_run_parser.set_defaults(func=tpv_dry_run_config_files)

    return parser


def configure_logging(verbosity_count):
    # Remove all handlers associated with the root logger object.
    # or basicConfig persists
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    # set global logging level
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.DEBUG if verbosity_count > 3 else logging.ERROR,
        format='%(levelname)-5s: %(name)s: %(message)s')
    # Set client log level
    if verbosity_count:
        log.setLevel(max(4 - verbosity_count, 1) * 10)
    else:
        log.setLevel(logging.INFO)


def main():
    parser = create_parser()
    args = parser.parse_args(sys.argv[1:])
    configure_logging(args.verbosity_count)
    # invoke subcommand
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
