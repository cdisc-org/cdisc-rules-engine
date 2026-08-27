import os
import re
import unittest
import openpyxl
from test_utils import run_command, tearDown
from cdisc_rules_engine.enums.default_file_paths import DefaultFilePaths


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
        """Test that omitting the required -s/--standard option fails."""
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

    def test_validate_required_v_option_missing(self):
        """Test that omitting the required -v/--version option fails."""
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

    def test_validate_with_minimum_required_options(self):
        """Test that supplying just the 3 required options (-s, -v, -dp),
        with no other flags, succeeds without producing errors.
        """
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

    def test_validate_without_all_required_options(self):
        """Test that passing only -d (no -s/-v) fails with a missing-option
        error."""
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
        """Test that nearly every validate flag can be supplied together
        without crashing. Deliberately passes both -d and -dp (mutually
        exclusive), so the expected outcome is a non-empty stderr; this does
        not assert anything about the rule/report content itself.
        """
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
            DefaultFilePaths.EXCEL_TEMPLATE_FILE.value,
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-o",
            "result.json",
            "-of",
            "json",
            "-rr",
            "-dxp",
            os.path.join("tests", "resources", "report_test_data", "define.xml"),
            "--whodrug",
            os.path.join("tests", "resources", "dictionaries", "whodrug"),
            "--meddra",
            os.path.join("tests", "resources", "dictionaries", "meddra"),
            "-lr",
            os.path.join("tests", "resources", "Rule-CG0027.json"),
            "-lr",
            os.path.join("tests", "resources", "CG0272.yml"),
            "-p",
            "bar",
        ]
        exit_code, stdout, stderr = run_command(args, False)
        self.assertNotEqual(stderr, "")

    def test_validate_local_rule(self):
        """Test that -r filters which rule(s) within a -lr local rules
        directory run.
        """
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.2",
            "-dp",
            os.path.join("resources", "datasets", "ae.xpt"),
            "-lr",
            os.path.join("tests", "resources", "rules"),
            "-r",
            "CORE-000473",
        ]
        exit_code, stdout, stderr = run_command(args, False)
        self.assertEqual(exit_code, 0)
        self.assertNotIn("error", stderr.lower())
        self.assertFalse(self.error_keyword in stdout)

    def test_validate_no_rules(self):
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("resources", "datasets", "ae.xpt"),
            "-lr",
            os.path.join("tests", "resources", "rules"),
            "-r",
            "CORE-000473",
        ]
        exit_code, stdout, stderr = run_command(args, False)
        self.assertEqual(exit_code, 1)
        self.assertIn(
            "no rules were selected for this standard/version",
            stderr.lower(),
        )

    def test_validate_local_exclude_rule(self):
        """Test that -er excludes a rule within a -lr local rules directory
        (counterpart to test_validate_local_rule's include-filter case)."""
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.2",
            "-dp",
            os.path.join("resources", "datasets", "ae.xpt"),
            "-lr",
            os.path.join("tests", "resources", "rules"),
            "-er",
            "CORE-000012",
            "-l",
            "error",
        ]
        exit_code, stdout, stderr = run_command(args, False)
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertFalse(self.error_keyword in stdout)

    def test_validate_include_exclude(self):
        """Test that passing both -r and -er together is rejected with exit
        code 2, regardless of whether the referenced rules exist locally."""
        args = [
            "python",
            "core.py",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("resources", "datasets", "ae.xpt"),
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
        self.assertEqual(exit_code, 2)
        self.assertIn(
            "cannot use both --rules and --exclude-rules flags together.", stderr
        )

    def test_validate_less_than_minimum_options(self):
        """Test that -s alone (no -v, no dataset) fails with a specific
        "missing option -v" error message."""
        args = ["python", "core.py", "validate", "-s", "sdtmig"]
        exit_code, stdout, stderr = run_command(args, False)
        self.assertNotEqual(exit_code, 0)
        self.assertIn(
            "\n\nerror: missing option '-v' / '--version'.\n",
            stderr,
        )

    def test_validate_output_format_json(self):
        """Test that -of json produces a successful run."""
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
        """Test that -of xlsx produces a successful run."""
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
        """Test that an unrecognized -of value fails."""
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
            "abc",
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertNotEqual(exit_code, 0)
        self.assertNotEqual(stderr, "")

    def test_validate_with_log_level_disabled(self):
        """Test that -l disabled produces a successful run."""
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
        """Test that -l info logging doesn't affect exit code/output when a
        specific rule is selected.
        """
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
            "-lr",
            os.path.join("tests", "resources", "library_rules", "CORE-000237.json"),
        ]
        exit_code, stdout, stderr = run_command(args, False)

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)

    def test_validate_with_log_level_error(self):
        """Test that -l error produces a successful run (with some stderr
        output, since "error" level still surfaces log lines here)."""
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
        """Test that -l critical produces a successful run."""
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
        """Test that -l warn produces a successful run with no "warning" text
        in stderr."""
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
        """Test that an unrecognized -l value fails."""
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

    def test_validate_high_value_ps(self):
        """Test that a -ps (pool size) value smaller than the CPU count
        doesn't break validation."""
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
        """Test that a valid -dxp/--define-xml-path produces a successful
        run."""
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
        """Test the shell-string command-invocation variant (shell=True) with
        a single -dp data source and a full option set succeeds."""
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
            f"-dxp {os.path.join('tests', 'resources', 'define.xml')} "
            f"-l error"
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertNotIn("error", stderr.lower())

    def test_validate_dummy_with_all_options(self):
        """Test that passing both -dp and -d (mutually exclusive) via the
        shell-string command variant fails with the expected error message
        format."""
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
            f"-dxp {os.path.join('tests', 'resources', 'define.xml')} "
            f"-l error"
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertEqual(exit_code, 2)
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
        """Test that passing neither -dp nor -d fails with the expected
        "you must pass one of" error message format."""
        args = (
            f"python core.py validate "
            f"-ca {os.path.join('resources', 'cache')} "
            f"-lr {os.path.join('tests', 'resources', 'Rule-CG0027.json')} "
            f"-s sdtmig "
            f"-v 3.4 "
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertEqual(exit_code, 2)
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
        """Test that omitting -ca/--cache-path falls back to the default
        cache location and still succeeds."""
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
        """Test that omitting the optional --whodrug/--meddra dictionary
        paths still succeeds."""
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
        """Test that invalid --whodrug/--meddra dictionary paths fail."""
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

    def test_validate_dummy_with_vx_as_no(self):
        """Test that -vx no disables Define-XML validation without error."""
        args = (
            f"python core.py validate "
            f"-s sendig "
            f"-v 3.1 "
            f"-lr {os.path.join('tests', 'resources', 'CoreIssue295', 'SEND4.json')} "
            f"-dp {os.path.join('tests', 'resources', 'CoreIssue295', 'dm.json')} "
            f"-vx no"
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertNotIn("error", stdout)

    def test_validate_dummy_with_vx_as_yes(self):
        """Test that -vx y (default) enables Define-XML validation and still
        succeeds."""
        args = (
            f"python core.py validate "
            f"-s sendig "
            f"-v 3.1 "
            f"-lr {os.path.join('tests', 'resources', 'CoreIssue295', 'SEND4.json')} "
            f"-dp {os.path.join('tests', 'resources', 'CoreIssue295', 'dm.json')} "
            f"-vx y"
        )
        exit_code, stdout, stderr = run_command(args, True)
        self.assertEqual(exit_code, 0)
        self.assertNotIn("error", stdout)

    def tearDown(self):
        tearDown()


if __name__ == "__main__":
    unittest.main()
