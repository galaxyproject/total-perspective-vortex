import argparse
import sys

from resources import ResourceDestinationParser


def create_parser():
    parser = argparse.ArgumentParser(
        description='Map resources (tools, users, roles) to dynamic destinations based on matching tags')
    parser.add_argument('-r', '--resource-destination-config', required=True,
                        help="Resource destination config file", )
    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    parser = ResourceDestinationParser.from_file_path(args.resource_destination_config)
    parser.print_destinations()
    return 0


if __name__ == '__main__':
    sys.exit(main())
