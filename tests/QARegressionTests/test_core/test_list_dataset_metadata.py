import os
import unittest
from click.testing import CliRunner

from core import list_dataset_metadata
from test_utils import tearDown


class TestListDatasetMetadata(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_list_dataset_metadata_with_valid_paths(self):
        """Test that list-dataset-metadata on a valid -dp file returns the
        expected metadata JSON (domain EX, correct filename)."""
        result = self.runner.invoke(
            list_dataset_metadata,
            ["-dp", os.path.join("tests", "resources", "test_dataset.xpt")],
        )
        expected_output = """[
    {
        "domain": "EX",
        "filename": "test_dataset.xpt","""
        self.assertEqual(result.exit_code, 0)
        self.assertIn(expected_output, result.output)

    def test_list_dataset_metadata_with_invalid_paths(self):
        """Test that a nonexistent -dp path fails (exit code 1) and does not
        produce dataset metadata output."""
        result = self.runner.invoke(list_dataset_metadata, ["-dp", "invalid_path"])
        expected_output = """[
    {
        "domain": "EX",
        "file_size": 823120,
        "label": "Exposure",
    """
        self.assertEqual(result.exit_code, 1)
        self.assertNotIn(expected_output, result.output)

    def test_list_dataset_metadata_with_no_paths(self):
        """Test that omitting the required -dp option fails with a
        missing-option error."""
        result = self.runner.invoke(list_dataset_metadata)
        expected_output = """Error: Missing option '-dp' / '--dataset-path'"""
        self.assertEqual(result.exit_code, 2)
        self.assertIn(expected_output, result.output)

    def tearDown(self):
        tearDown()


if __name__ == "__main__":
    unittest.main()
