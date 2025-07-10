"""Unit tests module for the helper functions"""

import unittest

from tpv.commands.test import mock_galaxy
from tpv.core.helpers import get_dataset_attributes


class TestHelpers(unittest.TestCase):
    """Tests for helper functions"""

    def test_get_dataset_attributes(self):
        """Test that the function returns a dictionary with the correct attributes"""
        job = mock_galaxy.Job()
        job.add_input_dataset(
            mock_galaxy.DatasetAssociation(
                "test",
                mock_galaxy.Dataset("test.txt", file_size=7 * 1024**3, object_store_id="files1"),
            )
        )
        dataset_attributes = get_dataset_attributes(job.input_datasets)
        expected_result = {0: {"object_store_id": "files1", "size": 7 * 1024**3}}
        self.assertEqual(dataset_attributes, expected_result)
