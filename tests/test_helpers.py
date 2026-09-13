"""Unit tests module for the helper functions"""

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tpv.commands.test import mock_galaxy
from tpv.core.helpers import (
    concurrent_job_count_for_tool,
    get_dataset_attributes,
    get_input_dataset,
    get_input_datasets,
    get_input_size,
    input_size,
    job_args_match,
    tool_version_eq,
    tool_version_gte,
    weighted_choice,
    weighted_random_sampling,
)


class TestHelpers(unittest.TestCase):
    """Tests for helper functions"""

    def test_get_dataset_attributes(self):
        """Test that the function returns a dictionary with the correct attributes"""
        job = mock_galaxy.Job()
        dataset = mock_galaxy.Dataset("test.txt", file_size=7 * 1024**3, object_store_id="files1")
        job.add_input_dataset(mock_galaxy.DatasetAssociation("test", dataset))
        dataset_attributes = get_dataset_attributes(job.input_datasets)
        expected_result = {dataset.id: {"object_store_id": "files1", "size": 7 * 1024**3}}
        self.assertEqual(dataset_attributes, expected_result)

    @staticmethod
    def _job_with_multiple_data_param():
        """A job with a `multiple="true"` data param `inputs` holding two datasets.

        Galaxy records the first dataset twice: once as `inputs` (the copy-metadata alias for
        the first element) and once as `inputs1`.
        """
        job = mock_galaxy.Job()
        first = mock_galaxy.DatasetAssociation(
            "first", mock_galaxy.Dataset("first.txt", file_size=3 * 1024**3), extension="txt"
        )
        second = mock_galaxy.DatasetAssociation(
            "second", mock_galaxy.Dataset("second.txt", file_size=5 * 1024**3), extension="txt"
        )
        job.add_input_dataset(first, name="inputs")
        job.add_input_dataset(first, name="inputs1")
        job.add_input_dataset(second, name="inputs2")
        return job

    def test_get_input_datasets_deduplicates_multiple_data_param_alias(self):
        """The `name`/`name1` alias of a multiple data param must not yield the dataset twice"""
        job = self._job_with_multiple_data_param()
        datasets = get_input_datasets(job, "inputs")
        self.assertEqual([dataset.name for dataset in datasets], ["first", "second"])

    def test_get_input_datasets_matches_collection_element_names(self):
        """A collection param is recorded as `name1`..`nameN`, with no unsuffixed entry"""
        job = mock_galaxy.Job()
        for index in range(1, 4):
            job.add_input_dataset(
                mock_galaxy.DatasetAssociation(
                    f"element{index}", mock_galaxy.Dataset(f"element{index}.txt", file_size=1 * 1024**3)
                ),
                name=f"inputs{index}",
            )
        datasets = get_input_datasets(job, "inputs")
        self.assertEqual([dataset.name for dataset in datasets], ["element1", "element2", "element3"])

    def test_get_input_datasets_ignores_other_params(self):
        """Only datasets recorded for the requested param are returned"""
        job = self._job_with_multiple_data_param()
        job.add_input_dataset(
            mock_galaxy.DatasetAssociation("ref", mock_galaxy.Dataset("ref.txt", file_size=9 * 1024**3)),
            name="reference",
        )
        self.assertEqual([dataset.name for dataset in get_input_datasets(job, "reference")], ["ref"])
        self.assertEqual([dataset.name for dataset in get_input_datasets(job, "inputs")], ["first", "second"])
        self.assertEqual([dataset.name for dataset in get_input_datasets(job)], ["first", "second", "ref"])

    def test_get_input_datasets_skips_unset_optional_params(self):
        """An unset optional data param is recorded with no dataset at all"""
        job = mock_galaxy.Job()
        job.input_datasets.append(mock_galaxy.JobToInputDatasetAssociation("inputs", None))
        self.assertEqual(get_input_datasets(job, "inputs"), [])
        self.assertIsNone(get_input_dataset(job, "inputs"))
        self.assertEqual(get_input_size(job, "inputs"), 0)

    def test_get_input_dataset_returns_first_match(self):
        job = self._job_with_multiple_data_param()
        dataset = get_input_dataset(job, "inputs")
        self.assertEqual(dataset.name, "first")
        self.assertIsNone(get_input_dataset(job, "nonexistent"))

    def test_get_input_size_totals_all_inputs_by_default(self):
        job = self._job_with_multiple_data_param()
        self.assertEqual(get_input_size(job), 8)
        # the existing input_size helper deduplicates the same way
        self.assertEqual(input_size(job), 8)

    def test_get_input_size_skips_inputs_whose_dataset_is_gone(self):
        # An input association can outlive its dataset (deleted or purged). It must contribute
        # nothing rather than raise mid-scheduling.
        job = self._job_with_multiple_data_param()
        job.add_input_dataset(mock_galaxy.DatasetAssociation("gone", None))
        self.assertEqual(get_input_size(job), 8)

    def test_get_input_size_by_param_name(self):
        job = self._job_with_multiple_data_param()
        job.add_input_dataset(
            mock_galaxy.DatasetAssociation("ref", mock_galaxy.Dataset("ref.txt", file_size=9 * 1024**3)),
            name="reference",
        )
        self.assertEqual(get_input_size(job, "inputs"), 8)
        self.assertEqual(get_input_size(job, "reference"), 9)
        self.assertEqual(get_input_size(job), 17)

    def test_get_input_size_adjusts_compressed_inputs(self):
        job = mock_galaxy.Job()
        job.add_input_dataset(
            mock_galaxy.DatasetAssociation(
                "compressed",
                mock_galaxy.Dataset("compressed.fastq.gz", file_size=2 * 1024**3),
                extension="fastqsanger.gz",
            ),
            name="inputs1",
        )
        job.add_input_dataset(
            mock_galaxy.DatasetAssociation(
                "uncompressed",
                mock_galaxy.Dataset("uncompressed.fastq", file_size=4 * 1024**3),
                extension="fastqsanger",
            ),
            name="inputs2",
        )
        self.assertAlmostEqual(get_input_size(job, "inputs"), 2 * 3.4 + 4)
        self.assertAlmostEqual(get_input_size(job, "inputs", compression_factor=2), 2 * 2 + 4)
        self.assertEqual(get_input_size(job, "inputs", estimate_uncompressed_size=False), 6)

    def test_weighted_random_sampling_without_weights_uses_unweighted_sampling(self):
        """When no destination defines params.weight, use unweighted random sampling."""
        destinations = [
            SimpleNamespace(id="dest_a", params={}),
            SimpleNamespace(id="dest_b", params=None),
            SimpleNamespace(id="dest_c", params={"foo": "bar"}),
        ]
        sampled_destinations = [destinations[2], destinations[0], destinations[1]]

        with patch("tpv.core.helpers.random.sample", return_value=sampled_destinations) as sample_mock:
            with patch("tpv.core.helpers.random.choices") as choices_mock:
                result = weighted_random_sampling(destinations)

        self.assertEqual(result, sampled_destinations)
        sample_mock.assert_called_once_with(destinations, k=3)
        choices_mock.assert_not_called()

    def test_weighted_random_sampling_with_weights_uses_weighted_choices(self):
        """When any destination defines params.weight, use weighted random choices."""
        destinations = [
            SimpleNamespace(id="dest_a", params={"weight": 5}),
            SimpleNamespace(id="dest_b", params={}),
            SimpleNamespace(id="dest_c", params=None),
        ]
        sampled_destinations = [destinations[0], destinations[0], destinations[2]]

        with patch("tpv.core.helpers.random.choices", return_value=sampled_destinations) as choices_mock:
            with patch("tpv.core.helpers.random.sample") as sample_mock:
                result = weighted_random_sampling(destinations)

        self.assertEqual(result, sampled_destinations)
        choices_mock.assert_called_once_with(destinations, weights=[5, 1, 1], k=3)
        sample_mock.assert_not_called()

    def test_weighted_choice_without_weights_uses_unweighted_choice(self):
        """When no item defines weight, use unweighted random choice."""
        items = [
            {"value": "/fast/jobs"},
            {"value": "/slow/jobs"},
            {"value": "/backup/jobs", "foo": "bar"},
        ]

        with patch("tpv.core.helpers.random.sample", return_value=[items[1]]) as sample_mock:
            with patch("tpv.core.helpers.random.choices") as choices_mock:
                result = weighted_choice(items)

        self.assertEqual(result, items[1])
        sample_mock.assert_called_once_with(items, k=1)
        choices_mock.assert_not_called()

    def test_weighted_choice_with_weights_uses_weighted_choices(self):
        """When any item defines weight, use weighted random choices."""
        items = [
            {"value": "/fast/jobs", "weight": 3},
            {"value": "/slow/jobs"},
            {"value": "/backup/jobs"},
        ]
        with patch("tpv.core.helpers.random.choices", return_value=[items[0]]) as choices_mock:
            with patch("tpv.core.helpers.random.sample") as sample_mock:
                result = weighted_choice(items)

        self.assertEqual(result, items[0])
        choices_mock.assert_called_once_with(items, weights=[3, 1, 1], k=1)
        sample_mock.assert_not_called()

    def test_weighted_choice_missing_weight_defaults_to_one(self):
        """Items without a weight key should default to weight 1."""
        items = [
            {"value": "/a", "weight": 5},
            {"value": "/b"},
            {"value": "/c"},
        ]
        with patch("tpv.core.helpers.random.choices", return_value=[items[1]]) as choices_mock:
            result = weighted_choice(items)

        self.assertEqual(result, items[1])
        choices_mock.assert_called_once_with(items, weights=[5, 1, 1], k=1)

    def test_weighted_choice_zero_weights_falls_back_to_unweighted(self):
        """If all weights are zero or negative, fall back to unweighted selection."""
        items = [
            {"value": "/drained/jobs", "weight": 0},
            {"value": "/also-drained/jobs", "weight": -1},
        ]
        with patch("tpv.core.helpers.random.sample", return_value=[items[0]]) as sample_mock:
            with patch("tpv.core.helpers.random.choices") as choices_mock:
                result = weighted_choice(items)

        self.assertEqual(result, items[0])
        sample_mock.assert_called_once_with(items, k=1)
        choices_mock.assert_not_called()

    def test_weighted_choice_negative_weight_is_clamped_to_zero(self):
        """A negative weight is clamped to 0 while positive weights are preserved."""
        items = [
            {"value": "/a", "weight": 5},
            {"value": "/b", "weight": -3},
        ]
        with patch("tpv.core.helpers.random.choices", return_value=[items[0]]) as choices_mock:
            with patch("tpv.core.helpers.random.sample") as sample_mock:
                weighted_choice(items)

        choices_mock.assert_called_once_with(items, weights=[5, 0], k=1)
        sample_mock.assert_not_called()

    def test_weighted_choice_default_weight_keys_use_unweighted(self):
        """If every item has weight: 1 (the default), use unweighted choice."""
        items = [
            {"value": "/a", "weight": 1},
            {"value": "/b", "weight": 1},
        ]
        with patch("tpv.core.helpers.random.sample", return_value=[items[0]]) as sample_mock:
            with patch("tpv.core.helpers.random.choices") as choices_mock:
                weighted_choice(items)

        sample_mock.assert_called_once_with(items, k=1)
        choices_mock.assert_not_called()

    def test_weighted_choice_empty_list_raises_value_error(self):
        """An empty list should raise ValueError."""
        with self.assertRaises(ValueError):
            weighted_choice([])

    def test_weighted_choice_returns_selected_item(self):
        """The helper should return the selected item, not a derived string value."""
        items = [
            {"value": "/primary/jobs", "weight": 10},
            {"value": "/secondary/jobs", "weight": 1},
        ]
        with patch("tpv.core.helpers.random.choices", return_value=[items[1]]):
            result = weighted_choice(items)

        self.assertIsInstance(result, dict)
        self.assertEqual(result, items[1])


class TestJobArgsMatch(unittest.TestCase):
    """job_args_match decides whether a rule fires on a job's parameters. Its only test so far is
    the positive case, which a helper that always returned True would pass."""

    def setUp(self):
        self.app = mock_galaxy.App(create_model=True)
        self.job = mock_galaxy.Job()
        self.job.param_values = {
            "input_opts": {"db_selector": "db", "tabs_to_spaces": False},
            "files": [{"name": "reads.fq"}],
        }

    def test_matches_only_when_every_given_value_matches(self):
        self.assertTrue(job_args_match(self.job, self.app, {"input_opts": {"db_selector": "db"}}))
        self.assertFalse(job_args_match(self.job, self.app, {"input_opts": {"db_selector": "other"}}))
        # one matching and one differing value is not a match
        self.assertFalse(
            job_args_match(self.job, self.app, {"input_opts": {"db_selector": "db", "tabs_to_spaces": True}})
        )

    def test_a_parameter_the_job_does_not_have_does_not_match(self):
        self.assertFalse(job_args_match(self.job, self.app, {"input_opts": {"no_such_option": 1}}))
        self.assertFalse(job_args_match(self.job, self.app, {"no_such_section": {"x": 1}}))
        # a nested dict in args where the job's parameter is a scalar is a mismatch, not an error
        self.assertFalse(job_args_match(self.job, self.app, {"files": {"name": "reads.fq"}}))

    def test_nothing_to_match_against_never_matches(self):
        for args in (None, {}, "input_opts", ["input_opts"]):
            with self.subTest(args=args):
                self.assertFalse(job_args_match(self.job, self.app, args))

    def test_a_list_valued_parameter_is_compared_as_a_whole(self):
        # Repeats and multi-selects are lists. The list is the value; keys inside its items are
        # not a path into it.
        self.assertTrue(job_args_match(self.job, self.app, {"files": [{"name": "reads.fq"}]}))
        self.assertFalse(job_args_match(self.job, self.app, {"files": [{"name": "other.fq"}]}))


class TestConcurrentJobCount(unittest.TestCase):
    def test_plain_tool_ids_match_exactly_and_do_not_count_prefix_collisions(self):
        # Tool Shed ids are matched by prefix so every installed version is counted together.
        # A plain id must not get that treatment: "bwa" must not count "bwa_mem" jobs.
        app = mock_galaxy.App(create_model=True)
        session = app.model.context
        user = app.model.User(username="arthur", email="arthur@vortex.org", password="x")
        session.add(user)
        session.flush()
        for tool_id in ("bwa", "bwa", "bwa_mem"):
            job = app.model.Job()
            job.user = user
            job.tool_id = tool_id
            job.state = "running"
            session.add(job)
        session.flush()

        self.assertEqual(concurrent_job_count_for_tool(app, mock_galaxy.Tool("bwa")), 2)
        self.assertEqual(concurrent_job_count_for_tool(app, mock_galaxy.Tool("bwa_mem")), 1)


class TestToolVersionComparison(unittest.TestCase):
    def test_versions_are_compared_as_versions_not_strings(self):
        tool = mock_galaxy.Tool("t", version="1.2.0")
        self.assertTrue(tool_version_eq(tool, "1.2"))  # PEP 440: 1.2.0 == 1.2
        self.assertTrue(tool_version_eq(tool, "1.2.0"))
        self.assertFalse(tool_version_eq(tool, "1.3"))
        self.assertTrue(tool_version_gte(tool, "1.10") is False)  # not a string compare: "1.2" < "1.10"

    def test_comparison_is_unknown_not_false_when_a_version_is_missing(self):
        # A rule can tell "no version to compare" (None) from "compared and did not match" (False).
        unversioned = mock_galaxy.Tool("t")
        self.assertIsNone(tool_version_eq(unversioned, "1.0"))
        self.assertIsNone(tool_version_gte(mock_galaxy.Tool("t", version="1.0"), None))
