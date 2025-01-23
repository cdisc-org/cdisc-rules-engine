import os
import unittest
import openpyxl
from test_utils import run_command


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
            "-l",
            "critical",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
        ]
        exit_code, stdout, stderr = run_command(args)

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
            "-l",
            "critical",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
        ]
        exit_code, stdout, stderr = run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)

    def test_validate_required_v_option_missing(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-l",
            "critical",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
        ]
        exit_code, stdout, stderr = run_command(args)

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
            "-l",
            "critical",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
        ]
        exit_code, stdout, stderr = run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertEqual(stderr, "")

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
            "-l",
            "critical",
        ]
        exit_code, stdout, stderr = run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertEqual(stderr, "")

    def test_validate_without_all_required_options(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-l",
            "critical",
            "-d",
            os.path.join("tests", "resources", "report_test_data"),
        ]
        exit_code, stdout, stderr = run_command(args)

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
        exit_code, stdout, stderr = run_command(args)
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
        exit_code, stdout, stderr = run_command(args)
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertFalse(self.error_keyword in stdout)

    def test_validate_minimum_options(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-l",
            "critical",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
        ]
        exit_code, stdout, stderr = run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertFalse(self.error_keyword in stdout)

    def test_validate_less_than_minimum_options(self):
        args = ["python", "core.py", "validate", "-s", "sdtmig", "-l", "critical"]
        exit_code, stdout, stderr = run_command(args)
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
            "-l",
            "critical",
        ]
        exit_code, stdout, stderr = run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertEqual(stderr, "")

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
            "-l",
            "critical",
        ]
        exit_code, stdout, stderr = run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertEqual(stderr, "")

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
            "-l",
            "critical",
        ]
        exit_code, stdout, stderr = run_command(args)

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
        exit_code, stdout, stderr = run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertEqual(stderr, "")

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
        exit_code, stdout, stderr = run_command(args)

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
        exit_code, stdout, stderr = run_command(args)

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
        exit_code, stdout, stderr = run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertEqual(stderr, "")

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
        exit_code, stdout, stderr = run_command(args)
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
        exit_code, stdout, stderr = run_command(args)

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
        exit_code, stdout, stderr = run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertEqual(stderr, "")

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
            "-l",
            "critical",
        ]
        exit_code, stdout, stderr = run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertEqual(stderr, "")

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
            "-l",
            "critical",
        ]
        exit_code, stdout, stderr = run_command(args)
        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertEqual(stderr, "")

    def tearDown(self):
        for file_name in os.listdir("."):
            if file_name != "host.json" and (
                file_name.endswith(".xlsx") or file_name.endswith(".json")
            ):
                os.remove(file_name)


if __name__ == "__main__":
    unittest.main()
