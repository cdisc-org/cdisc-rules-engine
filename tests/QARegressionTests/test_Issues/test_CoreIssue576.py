import subprocess
import os
import openpyxl
import pytest
from conftest import get_python_executable
from QARegressionTests.globals import (
    issue_datails_sheet,
    dataset_details_sheet,
    rules_report_sheet,
    issue_sheet_coreid_column,
    dataset_sheet_dataset_column,
    rules_sheet_rule_status_column,
)

""" These tests utilize positive and negative dataset
to validate successful working CG0202. Positive
dataset have supp datasets so rule should run
successfully. while Negative datasets have no
supp datasets so rule should be skipped. The
succesfuly running against postive dataset means
the scope skip problem is resolved """


@pytest.mark.regression
def test_negative_dataset():
    command = (
        f"{get_python_executable()} -m core test -s sdtmig -v 3.4 -r "
        + os.path.join("tests", "resources", "CoreIssue576", "Rule_underscores.json")
        + " -dp "
        + os.path.join("tests", "resources", "CoreIssue576", "Datasets_Negative.json")
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
    issues_sheet = workbook[issue_datails_sheet]
    dataset_sheet = workbook[dataset_details_sheet]
    rules_sheet = workbook[rules_report_sheet]

    coreid_column = issues_sheet[issue_sheet_coreid_column]
    dataset_column = dataset_sheet[dataset_sheet_dataset_column]
    rule_status_column = rules_sheet[rules_sheet_rule_status_column]

    coreid_values = [cell.value for cell in coreid_column[1:]]
    dataset_values = [cell.value for cell in dataset_column[1:]]
    rule_status_column_values = [cell.value for cell in rule_status_column[1:]]

    # Remove None values using list comprehension
    coreid_values = [value for value in coreid_values if value is not None]
    dataset_values = [value for value in dataset_values if value is not None]
    rule_status_column_values = [
        value for value in rule_status_column_values if value is not None
    ]

    # Perform the assertion
    assert dataset_values[0] == "dm.xpt"
    assert len(dataset_values) == 1

    assert len(coreid_values) == 0

    assert rule_status_column_values[0] == "SKIPPED"

    # Close the workbook
    workbook.close()

    # Delete the file
    os.remove(file_name)


@pytest.mark.regression
def test_positive_dataset():
    command = (
        f"{get_python_executable()} -m core test -s sdtmig -v 3.4 -r "
        + os.path.join("tests", "resources", "CoreIssue576", "Rule_underscores.json")
        + " -dp "
        + os.path.join("tests", "resources", "CoreIssue576", "Datasets_positive.json")
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
    issues_sheet = workbook[issue_datails_sheet]
    dataset_sheet = workbook[dataset_details_sheet]
    rules_sheet = workbook[rules_report_sheet]

    coreid_column = issues_sheet[issue_sheet_coreid_column]
    dataset_column = dataset_sheet[dataset_sheet_dataset_column]
    rule_status_column = rules_sheet[rules_sheet_rule_status_column]

    coreid_values = [cell.value for cell in coreid_column[1:]]
    dataset_values = [cell.value for cell in dataset_column[1:]]
    rule_status_column_values = [cell.value for cell in rule_status_column[1:]]

    # Remove None values using list comprehension
    coreid_values = [value for value in coreid_values if value is not None]
    dataset_values = [value for value in dataset_values if value is not None]
    rule_status_column_values = [
        value for value in rule_status_column_values if value is not None
    ]

    # Perform the assertion
    assert dataset_values[0] == "suppae.xpt"
    assert dataset_values[1] == "suppec.xpt"

    assert len(coreid_values) == 0

    assert rule_status_column_values[0] == "SUCCESS"

    # Close the workbook
    workbook.close()

    # Delete the file
    os.remove(file_name)
