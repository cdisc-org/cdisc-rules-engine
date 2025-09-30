import os
from core import list_rule_sets
import unittest
from click.testing import CliRunner
from test_utils import tearDown


class TestListRuleSets(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_list_rule_sets_valid_cache_path(self):
        result = self.runner.invoke(
            list_rule_sets, ["-c", os.path.join("resources", "cache")]
        )
        self.assertIn("SDTMIG, 3-2", result.output)
        self.assertIn("SDTMIG, 3-3", result.output)
        self.assertIn("SDTMIG, 3-4", result.output)
        self.assertIn("SENDIG, 3-1", result.output)

    def test_list_rule_sets_invalid_cache_path(self):
        result = self.runner.invoke(
            list_rule_sets, ["--cache-path", os.path.join("resources")]
        )
        self.assertNotEqual(result.exit_code, 0)

    def test_list_rule_sets_no_cache_path(self):
        result = self.runner.invoke(list_rule_sets, [])
        self.assertIn("SDTMIG, 3-2", result.output)
        self.assertIn("SDTMIG, 3-3", result.output)
        self.assertIn("SDTMIG, 3-4", result.output)
        self.assertIn("SENDIG, 3-1", result.output)

    def tearDown(self):
        tearDown()


if __name__ == "__main__":
    unittest.main()
