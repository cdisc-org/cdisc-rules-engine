import subprocess
import os
import openpyxl
import pytest
from conftest import get_python_executable
from QARegressionTests.globals import (
    issue_datails_sheet,
    issue_sheet_record_column,
    issue_sheet_variable_column,
    issue_sheet_values_column,
)


@pytest.mark.regression
def test_negative_dataset():
    command = (
        f"{get_python_executable()} -m core test -s sdtmig -v 3.4 -r "
        + os.path.join("tests", "resources", "CoreIssue164", "rule.json")
        + " -dp "
        + os.path.join("tests", "resources", "CoreIssue164", "Negative_Dataset.json")
    )

    # Construct the command
    command = command.split(" ")

    # Run the command in the terminal
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    file_name = stdout.decode().strip().split(": ")[1] + ".xlsx"
    # Open the Excel file
    workbook = openpyxl.load_workbook(file_name)

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
    assert len(record_values) == 2
    assert len(variables_values) == 2
    assert len(values_column_values) == 2

    # Ensure negatives are detected at correct rows in dataset
    assert record_values[0] == 1
    assert record_values[1] == 3

    # Ensure correct variable is marked as negative
    assert variables_values[0] == variables_values[1] == "DSDY"

    # Ensure correct values were marked negatives
    assert values_column_values[0] == "0"
    assert values_column_values[1] == "alolo"

    # Close the workbook
    workbook.close()

    # Delete the file
    os.remove(file_name)


@pytest.mark.regression
def test_positive_dataset():
    command = (
        f"{get_python_executable()} -m core test -s sdtmig -v 3.4 -r "
        + os.path.join("tests", "resources", "CoreIssue164", "rule.json")
        + " -dp "
        + os.path.join("tests", "resources", "CoreIssue164", "Positive_Dataset.json")
    )

    # Construct the command
    command = command.split(" ")

    # Run the command in the terminal
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    file_name = stdout.decode().strip().split(": ")[1] + ".xlsx"
    print(file_name)
    # Open the Excel file
    workbook = openpyxl.load_workbook(file_name)

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

    # Close the workbook
    workbook.close()

    # Delete the file
    os.remove(file_name)
