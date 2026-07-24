from unittest.mock import MagicMock
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.services.reporting.usdm_report_data import USDMReportData


def test_get_rules_report_data(mock_validation_results):
    report = USDMReportData(
        [],
        ["test"],
        mock_validation_results,
        10.1,
        MagicMock(define_xml_path=None, max_errors_per_rule=(None, False)),
    )
    report_data = report.get_rules_report_data()
    expected_reports = []
    for result in mock_validation_results:
        expected_reports.append(
            {
                "core_id": result.id,
                "version": "1",
                "cdisc_rule_id": result.cdisc_rule_id,
                "message": result.message,
                "status": ExecutionStatus.ISSUE_REPORTED.value.upper(),
            }
        )
    expected_reports = sorted(expected_reports, key=lambda x: x["core_id"])
    assert len(report_data) == len(expected_reports)
    for i, _ in enumerate(report_data):
        assert report_data[i] == expected_reports[i]


def test_get_detailed_data(mock_validation_results):
    report = USDMReportData(
        [],
        ["test"],
        mock_validation_results,
        10.1,
        MagicMock(
            define_xml_path=None,
            max_errors_per_rule=(None, False),
        ),
    )
    detailed_data = report.get_detailed_data()
    errors = [
        {
            "core_id": mock_validation_results[0].id,
            "cdisc_rule_id": mock_validation_results[0].cdisc_rule_id,
            "message": "AESTDY and DOMAIN are equal to test",
            "executability": "Fully Executable",
            "entity": "",
            "instance_id": None,
            "path": None,
            "attributes": ["AESTDY", "DOMAIN"],
            "values": ["test", "test"],
        },
        {
            "core_id": mock_validation_results[0].id,
            "cdisc_rule_id": mock_validation_results[0].cdisc_rule_id,
            "message": "AESTDY and DOMAIN are equal to test",
            "executability": "Fully Executable",
            "entity": "",
            "instance_id": None,
            "path": None,
            "attributes": ["AESTDY", "DOMAIN"],
            "values": ["test", "test"],
        },
        {
            "core_id": mock_validation_results[1].id,
            "cdisc_rule_id": mock_validation_results[1].cdisc_rule_id,
            "message": "TTVARs are wrong",
            "executability": "Partially Executable",
            "entity": "",
            "instance_id": None,
            "path": None,
            "attributes": ["TTVAR1", "TTVAR2"],
            "values": ["test", "test"],
        },
    ]
    errors = sorted(errors, key=lambda x: (x["core_id"], x["entity"]))
    assert len(errors) == len(detailed_data)
    for i, error in enumerate(errors):
        assert error == detailed_data[i]


def test_get_summary_data(mock_validation_results):
    report = USDMReportData(
        [],
        ["test"],
        mock_validation_results,
        10.1,
        MagicMock(
            define_xml_path=None,
            max_errors_per_rule=(None, False),
        ),
    )
    summary_data = report.get_summary_data()
    errors = [
        {
            "entity": "",
            "core_id": mock_validation_results[0].id,
            "cdisc_rule_id": mock_validation_results[0].cdisc_rule_id,
            "message": "AESTDY and DOMAIN are equal to test",
            "issues": 2,
        },
        {
            "entity": "",
            "core_id": mock_validation_results[1].id,
            "cdisc_rule_id": mock_validation_results[1].cdisc_rule_id,
            "message": "TTVARs are wrong",
            "issues": 1,
        },
    ]
    errors = sorted(errors, key=lambda x: (x["entity"], x["core_id"]))
    assert len(errors) == len(summary_data)
    for i, error in enumerate(errors):
        assert error == summary_data[i]


def test_get_csv_rows_header(mock_validation_results):
    report = USDMReportData(
        [],
        ["test"],
        mock_validation_results,
        10.1,
        MagicMock(define_xml_path=None, max_errors_per_rule=(None, False)),
    )
    header, _ = report.get_csv_rows()
    assert header == ["path", "attribute", "value"]


def test_get_csv_rows_produces_one_row_per_attribute(mock_validation_results):
    report = USDMReportData(
        [],
        ["test"],
        mock_validation_results,
        10.1,
        MagicMock(define_xml_path=None, max_errors_per_rule=(None, False)),
    )
    _, rows = report.get_csv_rows()
    # 3 errors total (2 from CORE1, 1 from CORE2), each with 2 attributes → 6 rows
    assert len(rows) == 6
    for row in rows:
        assert len(row) == 3


def test_get_csv_rows_row_values(mock_validation_results):
    report = USDMReportData(
        [],
        ["test"],
        mock_validation_results,
        10.1,
        MagicMock(define_xml_path=None, max_errors_per_rule=(None, False)),
    )
    _, rows = report.get_csv_rows()
    attributes = {r[1] for r in rows}
    assert attributes == {"AESTDY", "DOMAIN", "TTVAR1", "TTVAR2"}
    for row in rows:
        assert row[2] == "test"


def test_get_csv_rows_empty_path_when_not_set(mock_validation_results):
    report = USDMReportData(
        [],
        ["test"],
        mock_validation_results,
        10.1,
        MagicMock(define_xml_path=None, max_errors_per_rule=(None, False)),
    )
    _, rows = report.get_csv_rows()
    # mock errors have no 'path' key, so path defaults to ""
    assert all(r[0] == "" for r in rows)


def test_get_csv_rows_empty_results():
    report = USDMReportData(
        [],
        ["test"],
        [],
        0.0,
        MagicMock(define_xml_path=None, max_errors_per_rule=(None, False)),
    )
    header, rows = report.get_csv_rows()
    assert header == ["path", "attribute", "value"]
    assert rows == []


def test_get_csv_rows_preserves_blank_values_for_none_and_empty_string(
    mock_validation_results,
):
    mock_validation_results[0].results[0]["errors"][0]["value"]["AESTDY"] = None
    mock_validation_results[0].results[0]["errors"][0]["value"]["DOMAIN"] = ""
    report = USDMReportData(
        [],
        ["test"],
        mock_validation_results,
        10.1,
        MagicMock(define_xml_path=None, max_errors_per_rule=(None, False)),
    )

    _, rows = report.get_csv_rows()
    assert any(row[1] == "AESTDY" and row[2] == "" for row in rows)
    assert any(row[1] == "DOMAIN" and row[2] == "" for row in rows)

    details = report.get_detailed_data()
    detail_row = next(
        row
        for row in details
        if row["core_id"] == "CORE1" and row["attributes"] == ["AESTDY", "DOMAIN"]
    )
    assert detail_row["values"] == ["null", "null"]


def test_no_errors_when_none_value_in_one_of_the_records(mock_validation_results):
    # forcing None and str comparison in summary and details
    mock_validation_results[0].id = None
    mock_metadata_1 = MagicMock()

    mock_metadata_1.name = None
    mock_metadata_1.record_count = 1
    mock_metadata_2 = MagicMock()
    mock_metadata_2.name = "name"
    mock_metadata_2.record_count = 2

    report = USDMReportData(
        [mock_metadata_1, mock_metadata_2],
        ["test"],
        mock_validation_results,
        10.1,
        MagicMock(
            define_xml_path=None,
            max_errors_per_rule=(None, False),
        ),
    )
    summary_data = report.get_summary_data()
    assert len(summary_data) == 2
    for i, error in enumerate(summary_data):
        assert error == summary_data[i]
    details = report.get_detailed_data()
    assert len(details) == 3


def test_get_csv_rows_execution_error(mock_validation_results):
    mock_validation_results[1].results[0][
        "executionStatus"
    ] = ExecutionStatus.EXECUTION_ERROR.value
    mock_validation_results[1].results[0]["entity"] = "TT"
    mock_validation_results[1].results[0]["message"] = "TTVARs are wrong"
    mock_validation_results[1].results[0]["errors"] = [
        {"error": "Unexpected KeyError in rule execution"}
    ]
    report = USDMReportData(
        [],
        ["test"],
        mock_validation_results,
        10.1,
        MagicMock(define_xml_path=None, max_errors_per_rule=(None, False)),
    )
    _, rows = report.get_csv_rows()
    error_rows = [r for r in rows if r[1] == "EXECUTION_ERROR"]
    assert len(error_rows) == 1
    path, attribute, value = error_rows[0]
    assert path == "TT"
    assert value == "TTVARs are wrong - Unexpected KeyError in rule execution"
    issue_rows = [r for r in rows if r[1] != "EXECUTION_ERROR"]
    assert len(issue_rows) == 4


def test_get_csv_rows_execution_error_detailed_message(mock_validation_results):
    mock_validation_results[1].results[0][
        "executionStatus"
    ] = ExecutionStatus.EXECUTION_ERROR.value
    mock_validation_results[1].results[0]["entity"] = "json"
    mock_validation_results[1].results[0]["message"] = "rule execution error"
    detailed_message = (
        "\n  Error parsing JSONata Rule for Core Id: CORE-000998\n"
        "  AttributeError: 'Jsonata' object has no attribute 'lower'"
    )
    mock_validation_results[1].results[0]["errors"] = [
        {"error": "Rule format error", "message": detailed_message}
    ]
    report = USDMReportData(
        [],
        ["test"],
        mock_validation_results,
        10.1,
        MagicMock(define_xml_path=None, max_errors_per_rule=(None, False)),
    )
    _, rows = report.get_csv_rows()
    error_rows = [r for r in rows if r[1] == "EXECUTION_ERROR"]
    assert len(error_rows) == 1
    path, attribute, value = error_rows[0]
    assert path == "json"
    assert value == detailed_message
