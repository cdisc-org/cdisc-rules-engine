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

from core import list_rule_sets  # noqa: E402


class TestListRuleSets(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_list_rule_sets_valid_cache_path(self):
        result = self.runner.invoke(
            list_rule_sets, ["-c", os.path.join("resources", "cache")]
        )
        self.assertIn("sdtmig, 3-2", result.output)
        self.assertIn("sdtmig, 3-3", result.output)
        self.assertIn("sdtmig, 3-4", result.output)
        self.assertIn("sendig, 3-1", result.output)

    def test_list_rule_sets_invalid_cache_path(self):
        result = self.runner.invoke(
            list_rule_sets, ["--cache-path", os.path.join("resources")]
        )
        self.assertNotEqual(result.exit_code, 0)

    def test_list_rule_sets_no_cache_path(self):
        result = self.runner.invoke(list_rule_sets, [])
        self.assertIn("sdtmig, 3-2", result.output)
        self.assertIn("sdtmig, 3-3", result.output)
        self.assertIn("sdtmig, 3-4", result.output)
        self.assertIn("sendig, 3-1", result.output)

    def tearDown(self):
        tearDown()


if __name__ == "__main__":
    unittest.main()
