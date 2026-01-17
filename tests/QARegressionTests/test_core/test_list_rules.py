import json
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

from core import list_rules  # noqa: E402


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
