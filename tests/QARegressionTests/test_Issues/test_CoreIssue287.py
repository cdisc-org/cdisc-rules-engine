import os
import subprocess
import unittest
from openpyxl import load_workbook
import pytest
from conftest import get_python_executable


@pytest.mark.regression
class TestCoreIssue287(unittest.TestCase):
    def test_generate_excel_file(self):
        # Execute the command to generate the Excel file
        command = [
            f"{get_python_executable()}",
            "-m",
            "core",
            "test",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-r",
            os.path.join("tests", "resources", "CoreIssue287", "is_null_rule.json"),
            "-dp",
            os.path.join(
                "tests", "resources", "CoreIssue287", "dur_is_null_dataset.json"
            ),
        ]
        subprocess.run(command, check=True)

        # Find the latest Excel file
        files = os.listdir(".")
        excel_files = [file for file in files if file.endswith(".xlsx")]
        latest_file = max(excel_files, key=os.path.getctime)

        # Open the latest Excel file
        workbook = load_workbook(latest_file)
        sheet = workbook["Issue Details"]

        # Assertion for dataset column
        dataset_column = sheet["D"]
        assert dataset_column[1].value == "AE"

        # Assertion for variable(s) column
        variables_column = sheet["H"]
        assert (
            variables_column[1].value
            == "$variable_is_null, define_variable_has_no_data, define_variable_name"
        )

        # Assertion for Values(s) column
        values_column = sheet["I"]
        assert values_column[1].value == "False, Yes, AEDUR"

        # Close the workbook
        workbook.close()

        # Clean up the generated files
        # os.remove(latest_file)


if __name__ == "__main__":
    unittest.main()
