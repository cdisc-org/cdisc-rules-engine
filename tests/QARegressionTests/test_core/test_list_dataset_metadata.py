import os
import unittest
from click.testing import CliRunner

from core import list_dataset_metadata


class TestCLI(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_list_dataset_metadata_with_valid_paths(self):
        result = self.runner.invoke(
            list_dataset_metadata, ["-dp", r"tests/resources/test_dataset.xpt"]
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
        "filename": "tests//resources//test_dataset.xpt",
        "full_path": "tests//resources//test_dataset.xpt",
        "size": 823120,
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
        for file_name in os.listdir("."):
            if file_name != "host.json" and (
                file_name.endswith(".xlsx") or file_name.endswith(".json")
            ):
                os.remove(file_name)


if __name__ == "__main__":
    unittest.main()
