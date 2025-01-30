import os
import re
import unittest
from test_utils import run_command
from test_utils import tearDown


class TestTestCommand(unittest.TestCase):
    def setUp(self):
        self.error_keyword = "error"

    def test_test_command_with_all_options_one_data_source(self):
        args = (
            f"python core.py test "
            f"-c {os.path.join('resources', 'cache')} "
            f"-dp {os.path.join('tests', 'resources', 'CoreIssue164', 'Positive_Dataset.json')} "
            f"-r {os.path.join('tests', 'resources', 'Rule-CG0027.json')} "
            f"--whodrug "
            f"{os.path.join('tests', 'resources', 'dictionaries', 'whodrug')} "
            f"--meddra {os.path.join('tests', 'resources', 'dictionaries', 'meddra')} "
            f"-s sdtmig "
            f"-v 3.4 "
            f"-dv 2.1 "
            f"-dxp {os.path.join('tests', 'resources','define.xml')} "
            f"-l error"
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertEqual(stderr, "", f"Error while executing command:\n{stderr}")

    def test_test_command_with_all_options(self):
        args = (
            f"python core.py test "
            f"-c {os.path.join('resources', 'cache')} "
            f"-dp {os.path.join('tests', 'resources', 'CG0027-positive.json')} "
            f"-d {os.path.join('tests', 'resources', 'report_test_data')} "
            f"-r {os.path.join('tests', 'resources', 'Rule-CG0027.json')} "
            f"--whodrug "
            f"{os.path.join('tests', 'resources', 'dictionaries', 'whodrug')} "
            f"--meddra {os.path.join('tests', 'resources', 'dictionaries', 'meddra')} "
            f"-s sdtmig "
            f"-v 3.4 "
            f"-dv 2.1 "
            f"-dxp {os.path.join('tests', 'resources','define.xml')} "
            f"-l error"
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertFalse(self.error_keyword in stdout)
        expected_pattern = (
            r"\[error \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} - "
            r"core\.py:\d+\] - argument --dataset-path cannot be used together "
            r"with argument --data\n"
        )
        error_msg = (
            f"Error message format doesn't match expected pattern.\n"
            f"Actual: {stderr}\n"
            f"Expected pattern: {expected_pattern}"
        )
        self.assertTrue(re.match(expected_pattern, stderr), error_msg)

    def test_test_command_without_dataset_path(self):
        args = (
            f"python core.py test "
            f"-c {os.path.join('resources', 'cache')} "
            f"-r {os.path.join('tests', 'resources', 'Rule-CG0027.json')}"
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertEqual(exit_code, 0)
        expected_pattern = (
            r"\[error \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} - "
            r"core\.py:\d+\] - you must pass one of the following arguments: "
            r"--dataset-path, --data\n"
        )
        error_msg = (
            f"Error message format doesn't match expected pattern.\n"
            f"Actual: {stderr}\n"
            f"Expected pattern: {expected_pattern}"
        )
        self.assertTrue(re.match(expected_pattern, stderr), error_msg)

    def test_test_command_without_rule(self):
        args = (
            f"python core.py test "
            f"-c {os.path.join('resources', 'cache')} "
            f"-dp {os.path.join('tests', 'resources', 'CG0027-positive.json')}"
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertNotEqual(exit_code, 0)
        self.assertNotEqual(
            stderr, "", f"Error not raised while executing invalid command:\n{stderr}"
        )

    def test_test_command_with_default_cache_path(self):
        args = (
            f"python core.py test "
            f"-s sdtmig "
            f"-v 3.4 "
            f"-dp {os.path.join('tests', 'resources', 'CG0027-positive.json')} "
            f"-r {os.path.join('tests', 'resources', 'Rule-CG0027.json')}"
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertEqual(stderr, "", f"Error while executing command:\n{stderr}")

    def test_test_command_without_whodrug_and_meddra(self):
        args = (
            f"python core.py test "
            f"-s sdtmig "
            f"-v 3.4 "
            f"-c {os.path.join('resources', 'cache')} "
            f"-dp {os.path.join('tests', 'resources', 'CG0027-positive.json')} "
            f"-r {os.path.join('tests', 'resources', 'Rule-CG0027.json')}"
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertEqual(stderr, "", f"Error while executing command:\n{stderr}")

    def test_test_command_with_invalid_whodrug_and_meddra(self):
        args = (
            f"python core.py test "
            f"-c {os.path.join('resources', 'cache')} "
            f"-dp {os.path.join('tests', 'resources', 'CG0027-positive.json')} "
            f"-r {os.path.join('tests', 'resources', 'Rule-CG0027.json')} "
            f"--whodrug invalid_path "
            f"--meddra invalid_path"
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertNotEqual(exit_code, 0)
        self.assertNotEqual(stderr, "", f"Error while executing command:\n{stderr}")

    def test_test_command_with_vx_as_no(self):
        args = (
            f"python core.py test "
            f"-s sendig "
            f"-v 3.1 "
            f"-dv 2.1 "
            f"-r {os.path.join('tests','resources','CoreIssue295','SEND4.json')} "
            f"-dp {os.path.join('tests','resources','CoreIssue295','dm.json')} "
            f"-vx no"
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertNotIn("error", stdout)

    def test_test_command_with_vx_as_yes(self):
        args = (
            f"python core.py test "
            f"-s sendig "
            f"-v 3.1 "
            f"-dv 2.1 "
            f"-r {os.path.join('tests','resources','CoreIssue295','SEND4.json')} "
            f"-dp {os.path.join('tests','resources','CoreIssue295','dm.json')} "
            f"-vx y"
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertEqual(exit_code, 0)
        self.assertTrue(stderr == "")

    def tearDown(self):
        tearDown()


if __name__ == "__main__":
    unittest.main()
