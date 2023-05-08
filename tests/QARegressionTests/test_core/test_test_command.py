import os
from core import test
import unittest
from click.testing import CliRunner


class TestCLI(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_test_command_with_all_options(self):
        result = self.runner.invoke(
            test,
            [
                "-c",
                r"resources/cache",
                "-dp",
                r"tests/resources/CG0027-positive.json",
                "-r",
                r"tests/resources/rules.json",
                "--whodrug",
                r"tests/resources/dictionaries/whodrug",
                "--meddra",
                r"tests/resources/dictionaries/meddra",
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dv",
                r"tests/resources/report_test_data/define.xml",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_test_command_without_dataset_path(self):
        result = self.runner.invoke(
            test,
            [
                "-c",
                r"/resources/cache",
                "-r",
                r"tests/resources/rules.json",
            ],
        )
        self.assertNotEqual(result.exit_code, 0)

    def test_test_command_without_rule(self):
        result = self.runner.invoke(
            test,
            ["-c", r"/resources/cache", "-dp", r"tests/resources/CG0027-positive.json"],
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
                r"tests/resources/CG0027-positive.json",
                "-r",
                r"tests/resources/rules.json",
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
                r"resources/cache",
                "-dp",
                r"tests/resources/CG0027-positive.json",
                "-r",
                r"tests/resources/rules.json",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_test_command_with_invalid_whodrug_and_meddra(self):
        result = self.runner.invoke(
            test,
            [
                "-c",
                r"/resources/cache",
                "-dp",
                r"tests/resources/CG0027-positive.json",
                "-r",
                r"tests/resources/rules.json",
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
