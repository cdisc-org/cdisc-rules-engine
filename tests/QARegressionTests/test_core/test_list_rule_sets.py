import os
from core import list_rule_sets
import unittest
from click.testing import CliRunner
from test_utils import tearDown


class TestListRuleSets(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_list_rule_sets_lists_expected_standard_version_pairs(self):
        """Test that list-rule-sets lists the expected standard/version pairs
        both with an explicit valid -c/--cache-path and with -c omitted
        entirely (falls back to the default cache path).
        """
        expected_pairs = ["sdtmig, 3-2", "sdtmig, 3-3", "sdtmig, 3-4", "sendig, 3-1"]
        for args in (["-c", os.path.join("resources", "cache")], []):
            result = self.runner.invoke(list_rule_sets, args)
            for pair in expected_pairs:
                self.assertIn(pair, result.output)

    def test_list_rule_sets_invalid_cache_path(self):
        """Test that an invalid/incomplete --cache-path (a dir with no rule
        cache files) fails with a non-zero exit code."""
        result = self.runner.invoke(
            list_rule_sets, ["--cache-path", os.path.join("resources")]
        )
        self.assertNotEqual(result.exit_code, 0)

    def tearDown(self):
        tearDown()


if __name__ == "__main__":
    unittest.main()
