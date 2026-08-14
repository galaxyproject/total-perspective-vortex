"""Unit tests module for the helper functions"""

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tpv.commands.test import mock_galaxy
from tpv.core.helpers import (
    get_dataset_attributes,
    get_input_dataset,
    get_input_datasets,
    get_input_size,
    input_size,
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
