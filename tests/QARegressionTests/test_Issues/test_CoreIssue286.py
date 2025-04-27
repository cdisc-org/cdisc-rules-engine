import subprocess
import os
import openpyxl
import pytest
from conftest import get_python_executable


@pytest.mark.regression
def test_negative_case_VS_dataset():
    command = (
        f"{get_python_executable()} -m core test -s sdtmig -v 3.4 -r "
        + os.path.join("tests", "resources", "CoreIssue286", "rule.json")
        + " -dp "
        + os.path.join("tests", "resources", "CoreIssue286", "dataset.json")
    )

    # Construct the command
    command = command.split(" ")

    # Run the command in the terminal
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    # Get the generated Excel file name from the standard output
    file_name = stdout.decode().strip().split(": ")[1] + ".xlsx"

    # Open the Excel file
    workbook = openpyxl.load_workbook(file_name)

    # Go to the "Issue Summary" sheet
    sheet = workbook["Issue Summary"]

    # Check the "Dataset" column for values
    dataset_column = sheet["A"]  # Assuming the "Dataset" column is column D

    # Extract all values from the "Dataset" column (excluding the header)
    dataset_values = [cell.value for cell in dataset_column[1:]]

    # Remove None values using list comprehension
    dataset_values = [value for value in dataset_values if value is not None]

    # Perform the assertion
    assert len(dataset_values) == 1  # Ensure only one value
    assert dataset_values[0] == "vs.xpt"  # Ensure the value is "VS"

    # Close the workbook
    workbook.close()

    # Delete the file
    os.remove(file_name)
