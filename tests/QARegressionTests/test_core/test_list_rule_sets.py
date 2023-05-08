import os
from core import list_rule_sets
import unittest
from click.testing import CliRunner


class TestCLI(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_list_rule_sets_valid_cache_path(self):
        result = self.runner.invoke(list_rule_sets, ["-c", r"resources/cache"])
        self.assertIn("SDTMIG, 3-2", result.output)
        self.assertIn("SDTMIG, 3-3", result.output)
        self.assertIn("SDTMIG, 3-4", result.output)
        self.assertIn("SENDIG, 3-1", result.output)

    def test_list_rule_sets_invalid_cache_path(self):
        result = self.runner.invoke(list_rule_sets, ["--cache_path", r"resources"])
        self.assertNotEqual(result.exit_code, 0)

    def test_list_rule_sets_no_cache_path(self):
        result = self.runner.invoke(list_rule_sets, [])
        self.assertIn("SDTMIG, 3-2", result.output)
        self.assertIn("SDTMIG, 3-3", result.output)
        self.assertIn("SDTMIG, 3-4", result.output)
        self.assertIn("SENDIG, 3-1", result.output)

    def tearDown(self):
        for file_name in os.listdir("."):
            if file_name != "host.json" and (
                file_name.endswith(".xlsx") or file_name.endswith(".json")
            ):
                os.remove(file_name)


if __name__ == "__main__":
    unittest.main()
