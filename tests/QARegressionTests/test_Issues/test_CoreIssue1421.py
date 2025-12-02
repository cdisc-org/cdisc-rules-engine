import os
import subprocess
import openpyxl
import pytest
from conftest import get_python_executable
from QARegressionTests.globals import (
    dataset_details_sheet,
    issue_datails_sheet,
    rules_report_sheet,
    issue_sheet_variable_column,
    issue_sheet_coreid_column,
)


@pytest.mark.regression
def test_validate_define_xml_against_lib_metadata():
    command = [
        f"{get_python_executable()}",
        "-m",
        "core",
        "validate",
        "-s",
        "sdtmig",
        "-v",
        "3-4",
        "-dp",
        os.path.join(
            "tests",
            "resources",
            "CoreIssue1421",
            "Dataset.json",
        ),
        "-lr",
        os.path.join("tests", "resources", "CoreIssue1421", "Rule.yml"),
        "-dxp",
        os.path.join("tests", "resources", "CoreIssue1421", "Define.xml"),
    ]
    subprocess.run(command, check=True)

    # Get the latest created Excel file
    files = os.listdir()
    excel_files = [
        file
        for file in files
        if file.startswith("CORE-Report-") and file.endswith(".xlsx")
    ]
    excel_file_path = sorted(excel_files)[-1]
    # Open the Excel file
    workbook = openpyxl.load_workbook(excel_file_path)

    # Go to the "Issue Details" sheet
    sheet = workbook[issue_datails_sheet]

    variables_values_column = sheet[issue_sheet_variable_column]
    variables_values = [
        cell.value for cell in variables_values_column[1:] if cell.value is not None
    ]
    assert len(variables_values) == 1
    for value in variables_values:
        assert len(value.split(",")) == 6

    variables_names_column = sheet["H"]
    variables_names_values = [
        cell.value for cell in variables_names_column[1:] if cell.value is not None
    ]
    assert len(variables_names_values) == 1
    for value in variables_names_values:
        assert len(value.split(",")) == 6

    dataset_column = sheet["D"]
    dataset_column_values = [
        cell.value for cell in dataset_column[1:] if cell.value is not None
    ]
    assert sorted(set(dataset_column_values)) == ["dm.xpt"]

    core_id_column = sheet[issue_sheet_coreid_column]
    core_id_column_values = [
        cell.value for cell in core_id_column[1:] if cell.value is not None
    ]
    assert set(core_id_column_values) == {"CDISC.SDTMIG.CG0999"}

    # Go to the "Rules Report" sheet
    rules_values = [
        row for row in workbook[rules_report_sheet].iter_rows(values_only=True)
    ][1:]
    rules_values = [row for row in rules_values if any(row)]
    assert rules_values[0][0] == "CDISC.SDTMIG.CG0999"
    assert "SUCCESS" in rules_values[0]
    assert (
        rules_values[0][4]
        == "Issue with codelist definition in the Define-XML document."
    )

    # Go to the "Dataset Details" sheet
    dataset_sheet = workbook[dataset_details_sheet]
    dataset_values = [row for row in dataset_sheet.iter_rows(values_only=True)][1:]
    dataset_values = [row for row in dataset_values if any(row)]
    assert len(dataset_values) > 0
    dataset_names = set(row[0] for row in dataset_values if row[0] is not None)
    assert dataset_names == {"ae.xpt", "dm.xpt", "ec.xpt", "ex.xpt", "suppec.xpt"}
    expected_records = {
        "ae.xpt": 74,
        "dm.xpt": 18,
        "ec.xpt": 1590,
        "ex.xpt": 1583,
        "suppec.xpt": 13,
    }
    for row in dataset_values:
        dataset_name = row[0]
        records_count = row[-1]
        assert records_count == expected_records[dataset_name]

    # Go to the "Issue Summary" sheet
    issue_summary_sheet = workbook["Issue Summary"]
    summary_values = [row for row in issue_summary_sheet.iter_rows(values_only=True)][
        1:
    ]
    summary_values = [row for row in summary_values if any(row)]
    assert len(summary_values) == 1
    core_ids = set(row[1] for row in summary_values if row[1] is not None)
    assert core_ids == {"CDISC.SDTMIG.CG0999"}
    # Check Message and dataset columns
    assert (
        summary_values[0][2]
        == "Issue with codelist definition in the Define-XML document."
    )
    assert summary_values[0][0] == "dm.xpt"

    # Delete the excel file
    if os.path.exists(excel_file_path):
        os.remove(excel_file_path)
