import json
import os
from core import list_rules
from test_utils import tearDown

import unittest
from click.testing import CliRunner


class TestListRules(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_list_rules_all_options_provided(self):
        """Test that list-rules with -c, -s and -v all provided succeeds."""
        result = self.runner.invoke(
            list_rules,
            ["-c", os.path.join("resources", "cache"), "-s", "sdtmig", "-v", "3.4"],
        )
        self.assertEqual(result.exit_code, 0)

    def test_list_rules_standard_option_provided(self):
        """Test that list-rules with only -s provided succeeds."""
        result = self.runner.invoke(list_rules, ["-s", "sdtmig"])
        self.assertEqual(result.exit_code, 0)

    def test_list_rules_version_option_provided(self):
        """Test that list-rules with only -v provided succeeds."""
        result = self.runner.invoke(list_rules, ["-v", "3.4"])
        self.assertEqual(result.exit_code, 0)

    def test_list_rules_no_option_provided(self):
        """Test that list-rules with no options at all succeeds (lists all
        rules).
        """
        result = self.runner.invoke(list_rules)
        self.assertEqual(result.exit_code, 0)

    def test_list_rules_output_format(self):
        """Test that list-rules output is a JSON list of rule dicts."""
        result = self.runner.invoke(list_rules)
        output = json.loads(result.output)
        self.assertIsInstance(output, list)
        self.assertTrue(all(isinstance(rule, dict) for rule in output))

    def tearDown(self):
        tearDown()


if __name__ == "__main__":
    unittest.main()
