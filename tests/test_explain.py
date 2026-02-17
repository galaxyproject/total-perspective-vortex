import os
import unittest

import yaml

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
        """explain=False should return just the destination (no tuple)."""
        dry_runner = TPVDryRunner.from_params(
            job_conf=self._fixture_path("job_conf_dry_run.yml"),
            tool_id="bwa",
            tpv_confs=[self._fixture_path("mapping-rules.yml")],
            input_size=6,
        )
        result = dry_runner.run(explain=False)

        # When explain=False, returns just the JobDestination (not a tuple)
        self.assertNotIsInstance(result, tuple)
        self.assertIsNotNone(result)

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
