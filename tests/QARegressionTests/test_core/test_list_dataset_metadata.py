import os
import sys
import unittest
from pathlib import Path

from click.testing import CliRunner

# Add project root to path so we can import core
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from test_utils import tearDown  # noqa: E402

from core import list_dataset_metadata  # noqa: E402


class TestListDatasetMetadata(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_list_dataset_metadata_with_valid_paths(self):
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
        result = self.runner.invoke(list_dataset_metadata)
        expected_output = """Error: Missing option '-dp' / '--dataset-path'"""
        self.assertEqual(result.exit_code, 2)
        self.assertIn(expected_output, result.output)

    def tearDown(self):
        tearDown()


if __name__ == "__main__":
    unittest.main()
