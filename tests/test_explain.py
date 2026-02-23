import os
import unittest
from unittest.mock import MagicMock

import yaml
from galaxy.jobs.mapper import JobMappingException

from tpv.commands.dryrunner import TPVDryRunner
from tpv.core.explain import ExplainCollector, ExplainPhase


class TestExplainCollectorUnit(unittest.TestCase):
    """Unit tests for ExplainCollector."""

    def test_add_and_render(self):
        collector = ExplainCollector()
        collector.add_step(ExplainPhase.CONFIG_LOADING, "Loaded config: test.yml")
        collector.add_step(
            ExplainPhase.ENTITY_MATCHING,
            "Tool 'bwa': matched entity 'bwa'",
            "Entity ID: bwa",
        )

        output = collector.render()
        self.assertIn("TPV SCHEDULING DECISION TRACE", output)
        self.assertIn("Configuration Loading", output)
        self.assertIn("Loaded config: test.yml", output)
        self.assertIn("Entity Matching", output)
        self.assertIn("Tool 'bwa': matched entity 'bwa'", output)
        self.assertIn("Entity ID: bwa", output)
        # Steps should be numbered
        self.assertIn("[1]", output)
        self.assertIn("[2]", output)

    def test_from_context_present(self):
        collector = ExplainCollector()
        context = {ExplainCollector.CONTEXT_KEY: collector}
        result = ExplainCollector.from_context(context)
        self.assertIs(result, collector)

    def test_from_context_absent(self):
        context = {"some_key": "some_value"}
        result = ExplainCollector.from_context(context)
        self.assertIsNone(result)

    def test_phases_grouped(self):
        collector = ExplainCollector()
        collector.add_step(ExplainPhase.CONFIG_LOADING, "step 1")
        collector.add_step(ExplainPhase.CONFIG_LOADING, "step 2")
        collector.add_step(ExplainPhase.ENTITY_MATCHING, "step 3")
        collector.add_step(ExplainPhase.ENTITY_MATCHING, "step 4")
        collector.add_step(ExplainPhase.FINAL_RESULT, "step 5")

        output = collector.render()
        # Phase headers should appear exactly once each
        self.assertEqual(output.count("--- Configuration Loading ---"), 1)
        self.assertEqual(output.count("--- Entity Matching ---"), 1)
        self.assertEqual(output.count("--- Final Result ---"), 1)
        # Steps in each phase should be contiguous
        config_loading_pos = output.index("--- Configuration Loading ---")
        entity_matching_pos = output.index("--- Entity Matching ---")
        step1_pos = output.index("[1] step 1")
        step2_pos = output.index("[2] step 2")
        step3_pos = output.index("[3] step 3")
        self.assertGreater(step1_pos, config_loading_pos)
        self.assertGreater(step2_pos, step1_pos)
        self.assertGreater(step3_pos, entity_matching_pos)

    def test_empty_render(self):
        collector = ExplainCollector()
        output = collector.render()
        self.assertIn("TPV SCHEDULING DECISION TRACE", output)
        # Should still have header (2 lines) + footer (1 line) = 3 separator lines
        self.assertEqual(output.count("=" * 72), 3)

    def test_render_yaml(self):
        collector = ExplainCollector()
        collector.add_step(ExplainPhase.CONFIG_LOADING, "Loaded config: test.yml")
        collector.add_step(
            ExplainPhase.ENTITY_MATCHING,
            "Matched tool",
            "detail info",
        )

        output = collector.render_yaml()
        data = yaml.safe_load(output)
        self.assertIn("phases", data)
        self.assertIn("Configuration Loading", data["phases"])
        self.assertIn("Entity Matching", data["phases"])
        self.assertEqual(data["phases"]["Configuration Loading"][0]["message"], "Loaded config: test.yml")
        self.assertEqual(data["phases"]["Entity Matching"][0]["detail"], "detail info")

    def test_render_yaml_empty(self):
        collector = ExplainCollector()
        output = collector.render_yaml()
        data = yaml.safe_load(output)
        self.assertEqual(data["phases"], {})

    def test_detail_multiline(self):
        collector = ExplainCollector()
        collector.add_step(
            ExplainPhase.FINAL_RESULT,
            "Selected destination",
            "line1\nline2\nline3",
        )
        output = collector.render()
        # Each detail line should be indented
        self.assertIn("        line1", output)
        self.assertIn("        line2", output)
        self.assertIn("        line3", output)

    def test_match_failure_reason_gpus_exceed_max(self):
        """match_failure_reason should report when gpu request exceeds max_accepted_gpus."""
        dest = MagicMock()
        dest.abstract = False
        dest.max_accepted_cores = None
        dest.max_accepted_mem = None
        dest.max_accepted_gpus = 2.0
        dest.min_accepted_cores = None
        dest.min_accepted_mem = None
        dest.min_accepted_gpus = None

        entity = MagicMock()
        entity.cores = None
        entity.mem = None
        entity.gpus = 4.0

        reason = ExplainCollector.match_failure_reason(dest, entity)
        self.assertIn("gpus", reason)
        self.assertIn("max_accepted_gpus", reason)

    def test_match_failure_reason_unknown(self):
        """match_failure_reason returns 'unknown reason' when no specific condition is met."""
        dest = MagicMock()
        dest.abstract = False
        dest.max_accepted_cores = None
        dest.max_accepted_mem = None
        dest.max_accepted_gpus = None
        dest.min_accepted_cores = None
        dest.min_accepted_mem = None
        dest.min_accepted_gpus = None

        entity = MagicMock()
        entity.cores = None
        entity.mem = None
        entity.gpus = None
        entity.tpv_tags.match.return_value = True  # tags match, not a tag mismatch

        reason = ExplainCollector.match_failure_reason(dest, entity)
        self.assertEqual(reason, "unknown reason")


class TestDryRunExplain(unittest.TestCase):
    """Integration tests for dry-run with explain."""

    @staticmethod
    def _fixture_path(name):
        return os.path.join(os.path.dirname(__file__), f"fixtures/{name}")

    def test_dry_run_explain_basic(self):
        """explain=True should return a collector with steps from all major phases."""
        dry_runner = TPVDryRunner.from_params(
            job_conf=self._fixture_path("job_conf_dry_run.yml"),
            tool_id="bwa",
            user_email="fairycake@vortex.org",
            tpv_confs=[self._fixture_path("mapping-rules.yml")],
            input_size=6,
        )
        destination, collector = dry_runner.run(explain=True)

        self.assertIsNotNone(collector)
        self.assertIsNotNone(destination)
        self.assertEqual(destination.id, "k8s_environment")

        trace = collector.render()
        # All major phases should be present
        self.assertIn("Configuration Loading", trace)
        self.assertIn("Entity Matching", trace)
        self.assertIn("Entity Combining", trace)
        self.assertIn("Rule Evaluation", trace)
        self.assertIn("Destination Matching", trace)
        self.assertIn("Destination Ranking", trace)
        self.assertIn("Final Result", trace)

    def test_dry_run_explain_shows_matched_entities(self):
        """The trace should show which tool and user entities matched."""
        dry_runner = TPVDryRunner.from_params(
            job_conf=self._fixture_path("job_conf_dry_run.yml"),
            tool_id="bwa",
            user_email="fairycake@vortex.org",
            tpv_confs=[self._fixture_path("mapping-rules.yml")],
            input_size=6,
        )
        _, collector = dry_runner.run(explain=True)
        trace = collector.render()

        self.assertIn("bwa", trace)
        self.assertIn("fairycake@vortex.org", trace)

    def test_dry_run_explain_shows_rules(self):
        """The trace should show rules with match/no-match status."""
        dry_runner = TPVDryRunner.from_params(
            job_conf=self._fixture_path("job_conf_dry_run.yml"),
            tool_id="bwa",
            user_email="fairycake@vortex.org",
            tpv_confs=[self._fixture_path("mapping-rules.yml")],
            input_size=6,
        )
        _, collector = dry_runner.run(explain=True)
        trace = collector.render()

        # With input_size=6, the rule "input_size <= 10" should match
        self.assertIn("MATCHED", trace)
        # Other rules should show as not matched
        self.assertIn("not matched", trace)

    def test_dry_run_explain_shows_destination_rejection(self):
        """Rejected destinations should have reasons."""
        dry_runner = TPVDryRunner.from_params(
            job_conf=self._fixture_path("job_conf_dry_run.yml"),
            tool_id="bwa",
            user_email="fairycake@vortex.org",
            tpv_confs=[self._fixture_path("mapping-rules.yml")],
            input_size=6,
        )
        _, collector = dry_runner.run(explain=True)
        trace = collector.render()

        self.assertIn("REJECTED", trace)
        self.assertIn("MATCHED", trace)

    def test_dry_run_explain_shows_destination_ranking(self):
        """Ranked destinations should show scores."""
        dry_runner = TPVDryRunner.from_params(
            job_conf=self._fixture_path("job_conf_dry_run.yml"),
            tool_id="bwa",
            user_email="fairycake@vortex.org",
            tpv_confs=[self._fixture_path("mapping-rules.yml")],
            input_size=6,
        )
        _, collector = dry_runner.run(explain=True)
        trace = collector.render()

        self.assertIn("Destination Ranking", trace)
        self.assertIn("k8s_environment", trace)
        self.assertIn("score", trace)

    def test_dry_run_without_explain(self):
        """run() always returns a (destination, collector) tuple; collector is None when explain=False."""
        dry_runner = TPVDryRunner.from_params(
            job_conf=self._fixture_path("job_conf_dry_run.yml"),
            tool_id="bwa",
            tpv_confs=[self._fixture_path("mapping-rules.yml")],
            input_size=6,
        )
        destination, collector = dry_runner.run(explain=False)

        self.assertIsNotNone(destination)
        self.assertIsNone(collector)

    def test_dry_run_exceptions_propagate_without_explain(self):
        """Exceptions must propagate to the caller when explain=False."""
        dry_runner = TPVDryRunner.from_params(
            job_conf=self._fixture_path("job_conf_dry_run.yml"),
            tool_id="bwa",
            tpv_confs=[self._fixture_path("mapping-destinations.yml")],
            input_size=25,
        )
        with self.assertRaises(JobMappingException):
            dry_runner.run(explain=False)

    def test_dry_run_exceptions_captured_with_explain(self):
        """Exceptions must be captured and not re-raised when explain=True."""
        dry_runner = TPVDryRunner.from_params(
            job_conf=self._fixture_path("job_conf_dry_run.yml"),
            tool_id="bwa",
            tpv_confs=[self._fixture_path("mapping-destinations.yml")],
            input_size=25,
        )
        destination, collector = dry_runner.run(explain=True)

        self.assertIsNone(destination)
        self.assertIsNotNone(collector)

    def test_dry_run_explain_default_tool(self):
        """Explain should work for the default tool too."""
        dry_runner = TPVDryRunner.from_params(
            job_conf=self._fixture_path("job_conf_dry_run.yml"),
            tool_id="_default_",
            tpv_confs=[self._fixture_path("mapping-basic.yml")],
        )
        destination, collector = dry_runner.run(explain=True)

        self.assertIsNotNone(collector)
        trace = collector.render()
        self.assertIn("Entity Matching", trace)
        self.assertIn("Final Result", trace)

    def test_dry_run_explain_with_roles(self):
        """Explain should show role matching."""
        dry_runner = TPVDryRunner.from_params(
            job_conf=self._fixture_path("job_conf_dry_run.yml"),
            tool_id="toolshed.g2.bx.psu.edu/repos/iuc/towel/towel_coverage/42",
            user_email="slartibartfast@glacier.org",
            roles=["training"],
            tpv_confs=[self._fixture_path("mapping-rules.yml")],
            input_size=100,
        )
        destination, collector = dry_runner.run(explain=True)

        self.assertEqual(destination.id, "training")
        trace = collector.render()
        self.assertIn("training", trace)

    def test_dry_run_explain_with_wait_exception(self):
        """Explain should still produce a trace when TryNextDestinationOrWait is raised."""
        dry_runner = TPVDryRunner.from_params(
            job_conf=self._fixture_path("job_conf_dry_run.yml"),
            tool_id="three_core_test_tool",
            tpv_confs=[self._fixture_path("mapping-destinations.yml")],
            input_size=5,
        )
        destination, collector = dry_runner.run(explain=True)

        # destination should be None since mapping failed
        self.assertIsNone(destination)
        self.assertIsNotNone(collector)
        trace = collector.render()
        # Should show entity matching phase
        self.assertIn("Entity Matching", trace)
        # Should show destination evaluation
        self.assertIn("Destination Evaluation", trace)
        # Should show final result with error
        self.assertIn("Final Result", trace)
        # Should indicate what went wrong
        self.assertIn("deferred", trace.lower())

    def test_dry_run_explain_with_no_matching_destinations(self):
        """Explain should produce a trace when no destinations match at all."""
        dry_runner = TPVDryRunner.from_params(
            job_conf=self._fixture_path("job_conf_dry_run.yml"),
            tool_id="bwa",
            tpv_confs=[self._fixture_path("mapping-destinations.yml")],
            input_size=25,
        )
        destination, collector = dry_runner.run(explain=True)

        # destination should be None since all destinations failed
        self.assertIsNone(destination)
        self.assertIsNotNone(collector)
        trace = collector.render()
        self.assertIn("Entity Matching", trace)
        self.assertIn("Final Result", trace)
        # Should indicate the failure
        self.assertIn("No destinations", trace)

    def test_dry_run_explain_with_matching_role_entity(self):
        """Explain trace should show a matched role entity."""
        dry_runner = TPVDryRunner.from_params(
            job_conf=self._fixture_path("job_conf_dry_run.yml"),
            tool_id="bwa",
            user_email="nobody@unknown.org",
            roles=["training"],
            tpv_confs=[self._fixture_path("mapping-role.yml")],
        )
        destination, collector = dry_runner.run(explain=True)

        self.assertIsNotNone(collector)
        trace = collector.render()
        self.assertIn("Role: matched entity 'training'", trace)

    def test_dry_run_explain_with_unmatched_user(self):
        """Explain trace should note when a user has no matching entity."""
        dry_runner = TPVDryRunner.from_params(
            job_conf=self._fixture_path("job_conf_dry_run.yml"),
            tool_id="bwa",
            user_email="nobody@unknown.org",
            tpv_confs=[self._fixture_path("mapping-basic.yml")],
        )
        destination, collector = dry_runner.run(explain=True)

        self.assertIsNotNone(collector)
        trace = collector.render()
        self.assertIn("nobody@unknown.org': no matching entity", trace)

    def test_resolve_relative_config_paths_keeps_absolute_and_urls(self):
        """Absolute paths and URLs should be returned unchanged."""
        result = TPVDryRunner.resolve_relative_config_paths(
            ["/absolute/path.yml", "https://example.com/config.yml"],
            "/some/job_conf.yml",
        )
        self.assertEqual(result, ["/absolute/path.yml", "https://example.com/config.yml"])

    def test_from_params_roles_without_user_creates_default_user(self):
        """from_params with roles but no user_email should create a default anonymous user."""
        runner = TPVDryRunner.from_params(
            job_conf=self._fixture_path("job_conf_dry_run.yml"),
            roles=["some_role"],
            tpv_confs=[self._fixture_path("mapping-basic.yml")],
        )
        self.assertIsNotNone(runner.user)
        self.assertEqual(runner.user.email, "gargravarr@vortex.org")

    def test_from_params_without_tool_id_sets_tool_none(self):
        """from_params without tool_id should leave tool as None."""
        runner = TPVDryRunner.from_params(
            job_conf=self._fixture_path("job_conf_dry_run.yml"),
            tpv_confs=[self._fixture_path("mapping-basic.yml")],
        )
        self.assertIsNone(runner.tool)
