import argparse
import logging
import sys

from .loader import VortexConfigLoader

log = logging.getLogger(__name__)


def vortex_lint_config_file(args):
    try:
        VortexConfigLoader.from_url_or_path(args.config)
        log.info("lint successful.")
        return 0
    except Exception:
        log.info("lint failed.")
        return 1


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
        help='loads a vortex configuration file and checks it for syntax errors',
        description="The linter will check yaml syntax and compile python code blocks")
    lint_parser.add_argument(
        'config', type=str,
        help="Path to the vortex config file to lint. Can be a local path or http url.")
    lint_parser.set_defaults(func=vortex_lint_config_file)

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
