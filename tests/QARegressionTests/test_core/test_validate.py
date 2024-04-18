import os
import unittest
import openpyxl
import subprocess


class TestValidate(unittest.TestCase):
    def setUp(self):
        self.error_message = "error"

    def run_command(self, args):
        try:
            completed_process = subprocess.run(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=True,
            )
            return (
                completed_process.returncode,
                completed_process.stdout.lower(),
                completed_process.stderr.lower(),
            )
        except subprocess.CalledProcessError as e:
            return e.returncode, e.stdout.lower(), e.stderr.lower()

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
            "-m",
            "core",
            "validate",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
        ]
        exit_code, stdout, stderr = self.run_command(args)

        self.assertNotEqual(exit_code, 0)
        self.assertNotEqual(stderr, "", "Error Not raised for invalid command")

    def test_validate_required_s_option_present(self):
        args = [
            "python",
            "-m",
            "core",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
        ]
        exit_code, stdout, stderr = self.run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_message in stdout)
        self.assertTrue(self.check_issue_summary_tab_empty())

    def test_validate_required_v_option_missing(self):
        args = [
            "python",
            "-m",
            "core",
            "validate",
            "-s",
            "sdtmig",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
        ]
        exit_code, stdout, stderr = self.run_command(args)

        self.assertNotEqual(exit_code, 0)
        self.assertNotEqual(stderr, "", "Error Not raised for invalid command")

    def test_validate_required_v_option_present(self):
        args = [
            "python",
            "-m",
            "core",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
        ]
        exit_code, stdout, stderr = self.run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_message in stdout)
        self.assertEqual(stderr, "")
        self.assertTrue(self.check_issue_summary_tab_empty())

    def test_validate_with_all_required_options(self):
        args = [
            "python",
            "-m",
            "core",
            "validate",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
            "-s",
            "sdtmig",
            "-v",
            "3.4",
        ]
        exit_code, stdout, stderr = self.run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_message in stdout)
        self.assertEqual(stderr, "")
        self.assertTrue(self.check_issue_summary_tab_empty())

    def test_validate_without_all_required_options(self):
        args = [
            "python",
            "-m",
            "core",
            "validate",
            "-d",
            os.path.join("tests", "resources", "report_test_data"),
        ]
        exit_code, stdout, stderr = self.run_command(args)

        self.assertNotEqual(exit_code, 0)
        self.assertIn("error: missing option", stderr.lower())

    def test_validate_all_options(self):
        args = [
            "python",
            "-m",
            "core",
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
        exit_code, stdout, stderr = self.run_command(args)
        self.assertNotEqual(stderr, "")

    def test_validate_unpublished_rule(self):
        args = [
            "python",
            "-m",
            "core",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-d",
            os.path.join("tests", "resources", "report_test_data"),
            "-lr",
            os.path.join("tests", "resources", "CG0272.yml"),
        ]
        exit_code, stdout, stderr = self.run_command(args)
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertFalse(self.error_message in stdout)
        self.assertTrue(self.check_issue_summary_tab_empty())

    def test_validate_minimum_options(self):
        args = [
            "python",
            "-m",
            "core",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
        ]
        exit_code, stdout, stderr = self.run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertFalse(self.error_message in stdout)
        self.assertTrue(self.check_issue_summary_tab_empty())

    def test_validate_less_than_minimum_options(self):
        args = ["python", "-m", "core", "validate", "-s", "sdtmig"]
        exit_code, stdout, stderr = self.run_command(args)
        self.assertNotEqual(exit_code, 0)
        self.assertIn("error: missing option", stderr)

    def test_validate_output_format_json(self):
        args = [
            "python",
            "-m",
            "core",
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
        exit_code, stdout, stderr = self.run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_message in stdout)
        self.assertEqual(stderr, "")

    def test_validate_output_format_excel(self):
        args = [
            "python",
            "-m",
            "core",
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
        exit_code, stdout, stderr = self.run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_message in stdout)
        self.assertEqual(stderr, "")

    def test_validate_with_invalid_output_format(self):
        args = [
            "python",
            "-m",
            "core",
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
        exit_code, stdout, stderr = self.run_command(args)

        self.assertNotEqual(exit_code, 0)
        self.assertNotEqual(stderr, "")

    def test_validate_with_log_level_disabled(self):
        args = [
            "python",
            "-m",
            "core",
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
        exit_code, stdout, stderr = self.run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_message in stdout)
        self.assertEqual(stderr, "")
        self.assertTrue(self.check_issue_summary_tab_empty())

    def test_validate_with_log_level_info(self):
        args = [
            "python",
            "-m",
            "core",
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
        exit_code, stdout, stderr = self.run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_message in stdout)
        self.assertIn("warning", stderr)
        self.assertTrue(self.check_issue_summary_tab_empty())

    def test_validate_with_log_level_error(self):
        args = [
            "python",
            "-m",
            "core",
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
        exit_code, stdout, stderr = self.run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_message in stdout)
        self.assertNotEqual(stderr, "")
        self.assertTrue(self.check_issue_summary_tab_empty())

    def test_validate_with_log_level_critical(self):
        args = [
            "python",
            "-m",
            "core",
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
        exit_code, stdout, stderr = self.run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_message in stdout)
        self.assertEqual(stderr, "")
        self.assertTrue(self.check_issue_summary_tab_empty())

    def test_validate_with_log_level_warn(self):
        args = [
            "python",
            "-m",
            "core",
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
        exit_code, stdout, stderr = self.run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_message in stdout)
        self.assertIn("warning", stderr)
        self.assertTrue(self.check_issue_summary_tab_empty())

    def test_validate_with_invalid_log_level(self):
        args = [
            "python",
            "-m",
            "core",
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
        exit_code, stdout, stderr = self.run_command(args)

        self.assertNotEqual(exit_code, 0)
        self.assertNotEqual(stderr, "")

    def test_validate_with_no_log_level(self):
        args = [
            "python",
            "-m",
            "core",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("tests", "resources", "test_dataset.xpt"),
        ]
        exit_code, stdout, stderr = self.run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_message in stdout)
        self.assertEqual(stderr, "")
        self.assertTrue(self.check_issue_summary_tab_empty())

    def test_validate_high_value_ps(self):
        args = [
            "python",
            "-m",
            "core",
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
        exit_code, stdout, stderr = self.run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_message in stdout)
        self.assertEqual(stderr, "")
        self.assertTrue(self.check_issue_summary_tab_empty())

    def test_validate_define_xml_path(self):
        args = [
            "python",
            "-m",
            "core",
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
        exit_code, stdout, stderr = self.run_command(args)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_message in stdout)
        self.assertEqual(stderr, "")
        self.assertTrue(self.check_issue_summary_tab_empty())

    def tearDown(self):
        for file_name in os.listdir("."):
            if file_name != "host.json" and (
                file_name.endswith(".xlsx") or file_name.endswith(".json")
            ):
                os.remove(file_name)


if __name__ == "__main__":
    unittest.main()
