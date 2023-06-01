import os
from core import test
import unittest
from click.testing import CliRunner


class TestTestCommand(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_test_command_with_all_options(self):
        result = self.runner.invoke(
            test,
            [
                "-c",
                os.path.join("resources", "cache"),
                "-dp",
                os.path.join("tests", "resources", "CG0027-positive.json"),
                "-r",
                os.path.join("tests", "resources", "Rule-CG0027.json"),
                "--whodrug",
                os.path.join("tests", "resources", "dictionaries", "whodrug"),
                "--meddra",
                os.path.join("tests", "resources", "dictionaries", "meddra"),
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dv",
                os.path.join("tests", "resources", "report_test_data", "define.xml"),
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_test_command_without_dataset_path(self):
        result = self.runner.invoke(
            test,
            [
                "-c",
                os.path.join("resources", "cache"),
                "-r",
                os.path.join("tests", "resources", "Rule-CG0027.json"),
            ],
        )
        self.assertNotEqual(result.exit_code, 0)

    def test_test_command_without_rule(self):
        result = self.runner.invoke(
            test,
            [
                "-c",
                os.path.join("resources", "cache"),
                "-dp",
                os.path.join("tests", "resources", "CG0027-positive.json"),
            ],
        )
        self.assertNotEqual(result.exit_code, 0)

    def test_test_command_with_default_cache_path(self):
        result = self.runner.invoke(
            test,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dp",
                os.path.join("tests", "resources", "CG0027-positive.json"),
                "-r",
                os.path.join("tests", "resources", "Rule-CG0027.json"),
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_test_command_without_whodrug_and_meddra(self):
        result = self.runner.invoke(
            test,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-c",
                os.path.join("resources", "cache"),
                "-dp",
                os.path.join("tests", "resources", "CG0027-positive.json"),
                "-r",
                os.path.join("tests", "resources", "Rule-CG0027.json"),
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_test_command_with_invalid_whodrug_and_meddra(self):
        result = self.runner.invoke(
            test,
            [
                "-c",
                os.path.join("resources", "cache"),
                "-dp",
                os.path.join("tests", "resources", "CG0027-positive.json"),
                "-r",
                os.path.join("tests", "resources", "Rule-CG0027.json"),
                "--whodrug",
                "invalid_path",
                "--meddra",
                "invalid_path",
            ],
        )
        self.assertNotEqual(result.exit_code, 0)

    def tearDown(self):
        for file_name in os.listdir("."):
            if file_name != "host.json" and (
                file_name.endswith(".xlsx") or file_name.endswith(".json")
            ):
                os.remove(file_name)


if __name__ == "__main__":
    unittest.main()
