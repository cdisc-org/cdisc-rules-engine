import subprocess
import os
import openpyxl
import pytest
from conftest import get_python_executable


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
    Issues_Sheet = workbook["Issue Details"]
    Dataset_Sheet = workbook["Dataset Details"]
    Rules_Sheet = workbook["Rules Report"]

    COREID_Column = Issues_Sheet["A"]
    Dataset_Column = Dataset_Sheet["A"]
    Rule_Status_Column = Rules_Sheet["G"]

    COREID_Values = [cell.value for cell in COREID_Column[1:]]
    Dataset_Values = [cell.value for cell in Dataset_Column[1:]]
    Rule_Status_Column_values = [cell.value for cell in Rule_Status_Column[1:]]

    # Remove None values using list comprehension
    COREID_Values = [value for value in COREID_Values if value is not None]
    Dataset_Values = [value for value in Dataset_Values if value is not None]
    Rule_Status_Column_values = [
        value for value in Rule_Status_Column_values if value is not None
    ]

    # Perform the assertion
    assert Dataset_Values[0] == "dm.xpt"
    assert len(Dataset_Values) == 1

    assert len(COREID_Values) == 0

    assert Rule_Status_Column_values[0] == "SKIPPED"

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
    Issues_Sheet = workbook["Issue Details"]
    Dataset_Sheet = workbook["Dataset Details"]
    Rules_Sheet = workbook["Rules Report"]

    COREID_Column = Issues_Sheet["A"]
    Dataset_Column = Dataset_Sheet["A"]
    Rule_Status_Column = Rules_Sheet["G"]

    COREID_Values = [cell.value for cell in COREID_Column[1:]]
    Dataset_Values = [cell.value for cell in Dataset_Column[1:]]
    Rule_Status_Column_values = [cell.value for cell in Rule_Status_Column[1:]]

    # Remove None values using list comprehension
    COREID_Values = [value for value in COREID_Values if value is not None]
    Dataset_Values = [value for value in Dataset_Values if value is not None]
    Rule_Status_Column_values = [
        value for value in Rule_Status_Column_values if value is not None
    ]

    # Perform the assertion
    assert Dataset_Values[0] == "suppae.xpt"
    assert Dataset_Values[1] == "suppec.xpt"

    assert len(COREID_Values) == 0

    assert Rule_Status_Column_values[0] == "SUCCESS"

    # Close the workbook
    workbook.close()

    # Delete the file
    os.remove(file_name)
