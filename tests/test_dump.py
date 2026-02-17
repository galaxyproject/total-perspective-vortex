import os
import unittest

import yaml

from tpv.commands.dumper import TPVConfigDumper


class TestTPVConfigDumper(unittest.TestCase):
    """Unit tests for TPVConfigDumper."""

    @staticmethod
    def _fixture_path(name):
        return os.path.join(os.path.dirname(__file__), f"fixtures/{name}")

    def test_dump_text_single_config(self):
        config_path = self._fixture_path("mapping-basic.yml")
        dumper = TPVConfigDumper.from_url_or_path([config_path])
        output = dumper.dump()

        self.assertIn("TPV MERGED CONFIGURATION", output)
        self.assertIn("mapping-basic.yml", output)
        self.assertIn("--- Global ---", output)
        self.assertIn("default_inherits: default", output)
        self.assertIn("--- Tools ---", output)
        self.assertIn("default (abstract):", output)
        self.assertIn("bwa:", output)
        self.assertIn("--- Destinations ---", output)
        self.assertIn("local:", output)
        self.assertIn("k8s_environment:", output)

    def test_dump_text_multiple_configs(self):
        remote_path = self._fixture_path("mapping-merge-multiple-remote.yml")
        local_path = self._fixture_path("mapping-merge-multiple-local.yml")
        dumper = TPVConfigDumper.from_url_or_path([remote_path, local_path])
        output = dumper.dump()

        self.assertIn("TPV MERGED CONFIGURATION", output)
        # Both sources should be listed
        self.assertIn("mapping-merge-multiple-remote.yml", output)
        self.assertIn("mapping-merge-multiple-local.yml", output)
        # Merged tools from both configs should appear
        self.assertIn("bwa:", output)
        self.assertIn("default", output)
        # Destinations from both configs
        self.assertIn("local:", output)
        self.assertIn("k8s_environment:", output)
        self.assertIn("another_k8s_environment:", output)

    def test_dump_yaml(self):
        config_path = self._fixture_path("mapping-basic.yml")
        dumper = TPVConfigDumper.from_url_or_path([config_path])
        output = dumper.dump(output_format="yaml")

        data = yaml.safe_load(output)
        self.assertIn("_sources", data)
        self.assertEqual(len(data["_sources"]), 1)
        self.assertIn("tools", data)
        self.assertIn("destinations", data)

    def test_dump_text_shows_destination_details(self):
        config_path = self._fixture_path("mapping-basic.yml")
        dumper = TPVConfigDumper.from_url_or_path([config_path])
        output = dumper.dump()

        self.assertIn("max_accepted_cores:", output)
        self.assertIn("max_accepted_mem:", output)
        self.assertIn("runner: local", output)
        self.assertIn("runner: k8s", output)

    def test_dump_text_shows_env_and_params(self):
        config_path = self._fixture_path("mapping-rules.yml")
        dumper = TPVConfigDumper.from_url_or_path([config_path])
        output = dumper.dump()

        self.assertIn("env:", output)
        self.assertIn("params:", output)
