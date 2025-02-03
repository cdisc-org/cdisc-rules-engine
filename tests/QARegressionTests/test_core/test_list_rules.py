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
        result = self.runner.invoke(
            list_rules,
            ["-c", os.path.join("resources", "cache"), "-s", "sdtmig", "-v", "3.4"],
        )
        self.assertEqual(result.exit_code, 0)

    def test_list_rules_standard_option_provided(self):
        result = self.runner.invoke(list_rules, ["-s", "sdtmig"])
        self.assertEqual(result.exit_code, 0)

    def test_list_rules_version_option_provided(self):
        result = self.runner.invoke(list_rules, ["-v", "3.4"])
        self.assertEqual(result.exit_code, 0)

    def test_list_rules_local_cache(self):
        result = self.runner.invoke(list_rules, ["-lr"])
        self.assertEqual(result.exit_code, 1)
        self.assertEqual(result.exception.args[1], "No such file or directory")

    def test_list_rules_local_cache_local_rule_id(self):
        result = self.runner.invoke(list_rules, ["-lr", "-lri", "1"])
        self.assertEqual(result.exit_code, 1)
        self.assertEqual(result.exception.args[1], "No such file or directory")

    def test_list_rules_no_option_provided(self):
        result = self.runner.invoke(list_rules)
        self.assertEqual(result.exit_code, 0)

    def test_list_rules_required_options_provided(self):
        result = self.runner.invoke(list_rules, ["-s", "sdtmig", "-v", "3.4"])
        self.assertEqual(result.exit_code, 0)

    def test_list_rules_output_format(self):
        result = self.runner.invoke(list_rules)
        output = json.loads(result.output)
        self.assertIsInstance(output, list)
        self.assertTrue(all(isinstance(rule, dict) for rule in output))

    def tearDown(self):
        tearDown()


if __name__ == "__main__":
    unittest.main()
