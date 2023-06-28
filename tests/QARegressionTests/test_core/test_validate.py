import os
from core import validate
import unittest
from click.testing import CliRunner


class TestValidate(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_validate_required_s_option_missing(self):
        result = self.runner.invoke(
            validate,
            [
                "-v",
                "3.4",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
            ],
        )
        self.assertEqual(result.exit_code, 2)
        self.assertIn("Error: Missing option '-s'", result.output)

    def test_validate_required_s_option_present(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
            ],
        )
        print(result.output)
        self.assertEqual(result.exit_code, 0)

    def test_validate_required_v_option_missing(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
            ],
        )
        self.assertEqual(result.exit_code, 2)
        self.assertIn("Error: Missing option '-v'", result.output)

    def test_validate_required_v_option_present(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_both_d_and_data_options(self):
        result = self.runner.invoke(
            validate,
            [
                "-d",
                os.path.join("tests", "resources", "report_test_data"),
                "--data",
                "tests/resources/report_test_data",
                "-v",
                "3.4",
                "-s",
                "sdtmig",
            ],
        )
        self.assertNotEqual(result.exit_code, 0)

    def test_validate_neither_d_nor_data_options(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_with_all_required_options(self):
        result = self.runner.invoke(
            validate,
            [
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
                "-s",
                "sdtmig",
                "-v",
                "3.4",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_without_all_required_options(self):
        result = self.runner.invoke(
            validate,
            [
                "-d",
                os.path.join("tests", "resources", "report_test_data"),
            ],
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Error: Missing option", result.output)

    def test_validate_all_options(self):
        result = self.runner.invoke(
            validate,
            [
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
                os.path.join("tests", "resources", "CG0027-positive.json"),
                "-p",
                "bar",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_minimum_options(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_less_than_minimum_options(self):
        result = self.runner.invoke(validate, ["-s", "sdtmig"])
        self.assertEqual(result.exit_code, 2)
        self.assertIn("Error: Missing option", result.output)

    def test_validate_invalid_value_to_option_version(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-v",
                "version",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
                "-dv",
                "2.0",
            ],
        )
        self.assertEqual(result.exit_code, 1)

    def test_validate_valid_options(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
                "-dv",
                "/tests/resources/report_test_data/define.xml",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_output_format_json(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
                "-of",
                "json",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_output_format_excel(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
                "-of",
                "xlsx",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_with_invalid_output_format(self):
        result = self.runner.invoke(
            validate,
            [
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
            ],
        )
        self.assertNotEqual(result.exit_code, 0)

    def test_validate_with_log_level_debug(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
                "-l",
                "debug",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_with_log_level_info(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
                "-l",
                "info",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_with_log_level_error(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
                "-l",
                "error",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_with_log_level_critical(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
                "-l",
                "critical",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_with_log_level_disabled(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
                "-l",
                "disabled",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_with_log_level_warn(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
                "-l",
                "warn",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_with_invalid_log_level(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
                "-l",
                "invalid",
            ],
        )
        self.assertNotEqual(result.exit_code, 0)

    def test_validate_with_no_log_level(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_high_value_p(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
                "-ps",
                "10",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_define_xml_path(self):
        result = self.runner.invoke(
            validate,
            [
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
                "-dxp",
                "/test/resources",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def tearDown(self):
        for file_name in os.listdir("."):
            if file_name != "host.json" and (
                file_name.endswith(".xlsx") or file_name.endswith(".json")
            ):
                os.remove(file_name)


if __name__ == "__main__":
    unittest.main()
