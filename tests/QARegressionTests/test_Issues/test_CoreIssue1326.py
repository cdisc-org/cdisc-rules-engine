import os
import subprocess
import unittest
import openpyxl
import pytest
from conftest import get_python_executable
from QARegressionTests.globals import (
    issue_datails_sheet,
    issue_sheet_record_column,
    issue_sheet_variable_column,
    issue_sheet_values_column,
)


# @pytest.mark.regression
class TestPrefTerm(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Run the command in the terminal
        command = [
            f"{get_python_executable()}",
            "-m",
            "core",
            "validate",
            "-s",
            "usdm",
            "-v",
            "3-0",
            "-dp",
            os.path.join("tests", "resources", "CoreIssue1326", "regression-test-coreid-DDF00015-positive.json"),
            "-lr",
            os.path.join("tests", "resources", "CoreIssue1326", "rule.yml")
        ]
        subprocess.run(command, check=True)

        # Get the latest created Excel file
        files = os.listdir()
        excel_files = [
            file
            for file in files
            if file.startswith("CORE-Report-") and file.endswith(".xlsx")
        ]
        cls.excel_file_path = sorted(excel_files)[-1]



    @classmethod
    def tearDownClass(cls):
        # Delete the Excel file
        if os.path.exists(cls.excel_file_path):
            os.remove(cls.excel_file_path)

    def test_positive_dataset(self):
        # # Open the Excel file
        workbook = openpyxl.load_workbook(self.excel_file_path)

        # Go to the "Issue Details" sheet
        sheet = workbook[issue_datails_sheet]

        record_column = sheet[issue_sheet_record_column]
        variables_column = sheet[issue_sheet_variable_column]
        values_column = sheet[issue_sheet_values_column]

        record_values = [cell.value for cell in record_column[1:]]
        variables_values = [cell.value for cell in variables_column[1:]]
        values_column_values = [cell.value for cell in values_column[1:]]

        # Remove None values using list comprehension
        record_values = [value for value in record_values if value is not None]
        variables_values = [value for value in variables_values if value is not None]
        values_column_values = [
            value for value in values_column_values if value is not None
        ]

        # Perform the assertion
        # Ensure only two negative values are caught
        assert len(record_values) == 0
        assert len(variables_values) == 0
        assert len(values_column_values) == 0


# if __name__ == "__main__":
#     unittest.main()
