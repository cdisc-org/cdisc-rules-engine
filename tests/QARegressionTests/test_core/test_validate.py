import os
import re
import unittest
import openpyxl
from test_utils import run_command, tearDown


class TestValidate(unittest.TestCase):
    def setUp(self):
        self.error_keyword = "error"

    def check_issue_summary_tab_empty(self):
        excel_files = [file for file in os.listdir(".") if file.endswith(".xlsx")]
        latest_excel_files = [
            file
            for file in excel_files
            if os.path.getctime(file)
            == max(os.path.getctime(file) for file in excel_files)
        ]

        if not latest_excel_files:
            return False

        latest_created_excel_file = latest_excel_files[0]

        workbook = openpyxl.load_workbook(latest_created_excel_file)
        issue_summary_tab = workbook["Issue Summary"]
        all_rows = issue_summary_tab.iter_rows(min_row=2)

        for row in all_rows:
            if any(cell.value is not None for cell in row):
                return False

        return True

    def test_validate_required_s_option_missing(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertNotEqual(exit_code, 0)
        self.assertNotEqual(stderr, "", "Error Not raised for invalid command")

    def test_validate_required_s_option_present(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)

    def test_validate_required_v_option_missing(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertNotEqual(exit_code, 0)
        self.assertNotEqual(stderr, "", "Error Not raised for invalid command")

    def test_validate_required_v_option_present(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertNotIn("error", stderr.lower())

    def test_validate_with_all_required_options(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
            "-s",
            "sdtmig",
            "-v",
            "3.4",
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertNotIn("error", stderr.lower())

    def test_validate_without_all_required_options(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-d",
            os.path.join("tests", "resources", "report_test_data"),
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertNotEqual(exit_code, 0)
        self.assertIn("error: missing option", stderr.lower())

    def test_validate_all_options(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-ca",
            os.path.join("resources", "cache"),
            "-ps",
            "20",
            "-d",
            os.path.join("tests", "resources", "report_test_data"),
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
            "-l",
            "debug",
            "-rt",
            os.path.join("resources", "templates", "report-template.xlsx"),
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-o",
            "result.json",
            "-of",
            "json",
            "-rr",
            "-dv",
            os.path.join("tests", "resources", "report_test_data", "define.xml"),
            "--whodrug",
            os.path.join("tests", "resources", "dictionaries", "whodrug"),
            "--meddra",
            os.path.join("tests", "resources", "dictionaries", "meddra"),
            "-r",
            os.path.join("tests", "resources", "Rule-CG0027.json"),
            "-lr",
            os.path.join("tests", "resources", "CG0272.yml"),
            "-p",
            "bar",
        ]
        exit_code, stdout, stderr = run_command(args, False)
        self.assertNotEqual(stderr, "")

    def test_validate_local_rule(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "datasets", "ae.xpt"),
            "-lr",
            os.path.join("tests", "resources", "rules"),
            "-r",
            "CORE-000473",
        ]
        exit_code, stdout, stderr = run_command(args, False)
        self.assertEqual(exit_code, 0)
        self.assertNotIn("error", stderr.lower())
        self.assertFalse(self.error_keyword in stdout)

    def test_validate_local_exclude_rule(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "datasets", "ae.xpt"),
            "-lr",
            os.path.join("tests", "resources", "rules"),
            "-er",
            "CORE-000473",
            "-l",
            "error",
        ]
        exit_code, stdout, stderr = run_command(args, False)
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertFalse(self.error_keyword in stdout)

    def test_validate_include_exclude(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "datasets", "ae.xpt"),
            "-lr",
            os.path.join("tests", "resources", "rules"),
            "-r",
            "CORE-000470",
            "-er",
            "CORE-000473",
            "-l",
            "error",
        ]
        exit_code, stdout, stderr = run_command(args, False)
        self.assertEqual(exit_code, 0)
        self.assertIn(
            "cannot use both --rules and --exclude-rules flags together.", stderr
        )

    def test_validate_minimum_options(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertEqual(exit_code, 0)
        self.assertNotIn("error", stderr.lower())
        self.assertFalse(self.error_keyword in stdout)

    def test_validate_less_than_minimum_options(self):
        args = ["python", "core.py", "validate", "-s", "sdtmig"]
        exit_code, stdout, stderr = run_command(args, False)
        self.assertNotEqual(exit_code, 0)
        self.assertIn("error: missing option", stderr)

    def test_validate_output_format_json(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
            "-of",
            "json",
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertNotIn("error", stderr.lower())

    def test_validate_output_format_excel(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
            "-of",
            "xlsx",
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertNotIn("error", stderr.lower())

    def test_validate_with_invalid_output_format(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
            "-o",
            "output.json",
            "-of",
            "csv",
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertNotEqual(exit_code, 0)
        self.assertNotEqual(stderr, "")

    def test_validate_with_log_level_disabled(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
            "-l",
            "disabled",
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertNotIn("error", stderr.lower())

    def test_validate_with_log_level_info(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
            "-l",
            "info",
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertIn("warning", stderr)

    def test_validate_with_log_level_error(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
            "-l",
            "error",
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertNotEqual(stderr, "")

    def test_validate_with_log_level_critical(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
            "-l",
            "critical",
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertNotIn("error", stderr.lower())

    def test_validate_with_log_level_warn(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
            "-l",
            "warn",
        ]
        exit_code, stdout, stderr = run_command(args, False)
        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertNotIn("warning", stderr)

    def test_validate_with_invalid_log_level(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
            "-l",
            "invalid",
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertNotEqual(exit_code, 0)
        self.assertNotEqual(stderr, "")

    def test_validate_with_no_log_level(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertNotIn("error", stderr.lower())

    def test_validate_high_value_ps(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
            "-ps",
            "10",
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertNotIn("error", stderr.lower())

    def test_validate_define_xml_path(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
            "-dxp",
            os.path.join("tests", "resources", "define.xml"),
        ]
        exit_code, stdout, stderr = run_command(args, False)
        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertNotIn("error", stderr.lower())

    def test_validate_dummy_with_all_options_one_data_source(self):
        args = (
            f"python core.py validate "
            f"-ca {os.path.join('resources', 'cache')} "
            f"-dp {os.path.join('tests', 'resources', 'CoreIssue164', 'Positive_Dataset.json')} "
            f"-lr {os.path.join('tests', 'resources', 'Rule-CG0027.json')} "
            f"--whodrug "
            f"{os.path.join('tests', 'resources', 'dictionaries', 'whodrug')} "
            f"--meddra {os.path.join('tests', 'resources', 'dictionaries', 'meddra')} "
            f"-s sdtmig "
            f"-v 3.4 "
            f"-dv 2.1 "
            f"-dxp {os.path.join('tests', 'resources', 'define.xml')} "
            f"-l error"
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertNotIn("error", stderr.lower())

    def test_validate_dummy_with_all_options(self):
        args = (
            f"python core.py validate "
            f"-ca {os.path.join('resources', 'cache')} "
            f"-dp {os.path.join('tests', 'resources', 'CG0027-positive.json')} "
            f"-d {os.path.join('tests', 'resources', 'report_test_data')} "
            f"-lr {os.path.join('tests', 'resources', 'Rule-CG0027.json')} "
            f"--whodrug "
            f"{os.path.join('tests', 'resources', 'dictionaries', 'whodrug')} "
            f"--meddra {os.path.join('tests', 'resources', 'dictionaries', 'meddra')} "
            f"-s sdtmig "
            f"-v 3.4 "
            f"-dv 2.1 "
            f"-dxp {os.path.join('tests', 'resources', 'define.xml')} "
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

    def test_validate_dummy_without_dataset_path(self):
        args = (
            f"python core.py validate "
            f"-ca {os.path.join('resources', 'cache')} "
            f"-lr {os.path.join('tests', 'resources', 'Rule-CG0027.json')} "
            f"-s sdtmig "
            f"-v 3.4 "
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

    def test_validate_dummy_with_default_cache_path(self):
        args = (
            f"python core.py validate "
            f"-s sdtmig "
            f"-v 3.4 "
            f"-dp {os.path.join('tests', 'resources', 'CG0027-positive.json')} "
            f"-lr {os.path.join('tests', 'resources', 'Rule-CG0027.json')}"
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertNotIn("error", stderr.lower())

    def test_validate_dummy_without_whodrug_and_meddra(self):
        args = (
            f"python core.py validate "
            f"-s sdtmig "
            f"-v 3.4 "
            f"-ca {os.path.join('resources', 'cache')} "
            f"-dp {os.path.join('tests', 'resources', 'CG0027-positive.json')} "
            f"-lr {os.path.join('tests', 'resources', 'Rule-CG0027.json')}"
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertNotIn("error", stderr.lower())

    def test_validate_dummy_with_invalid_whodrug_and_meddra(self):
        args = (
            f"python core.py validate "
            f"-ca {os.path.join('resources', 'cache')} "
            f"-dp {os.path.join('tests', 'resources', 'CG0027-positive.json')} "
            f"-lr {os.path.join('tests', 'resources', 'Rule-CG0027.json')} "
            f"--whodrug invalid_path "
            f"--meddra invalid_path"
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertNotEqual(exit_code, 0)
        self.assertNotEqual(stderr, "")

    def test_validate_dummy_without_vx(self):
        args = (
            f"python core.py validate "
            f"-s sendig "
            f"-v 3.1 "
            f"-dv 2.1 "
            f"-lr {os.path.join('tests', 'resources', 'CoreIssue295', 'SEND4.json')} "
            f"-dp {os.path.join('tests', 'resources', 'CoreIssue295', 'dm.json')} "
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertNotIn("error", stdout)

    def test_validate_dummy_with_vx(self):
        args = (
            f"python core.py validate "
            f"-s sendig "
            f"-v 3.1 "
            f"-dv 2.1 "
            f"-lr {os.path.join('tests', 'resources', 'CoreIssue295', 'SEND4.json')} "
            f"-dp {os.path.join('tests', 'resources', 'CoreIssue295', 'dm.json')} "
            f"-vx"
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertEqual(exit_code, 0)
        self.assertNotIn("error", stdout)

    def tearDown(self):
        tearDown()


if __name__ == "__main__":
    unittest.main()
