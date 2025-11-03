from unittest.mock import MagicMock
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.services.reporting.sdtm_report_data import SDTMReportData


def test_get_rules_report_data(mock_validation_results):
    report = SDTMReportData(
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
                "fda_rule_id": result.fda_rule_id,
                "message": result.message,
                "status": ExecutionStatus.SUCCESS.value.upper(),
            }
        )
    expected_reports = sorted(expected_reports, key=lambda x: x["core_id"])
    assert len(report_data) == len(expected_reports)
    for i, _ in enumerate(report_data):
        assert report_data[i] == expected_reports[i]


def test_get_detailed_data(mock_validation_results):
    report = SDTMReportData(
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
            "message": "AESTDY and DOMAIN are equal to test",
            "executability": "Fully Executable",
            "dataset": None,
            "USUBJID": "CDISC002",
            "row": 1,
            "SEQ": 2,
            "variables": ["AESTDY", "DOMAIN"],
            "values": ["test", "test"],
        },
        {
            "core_id": mock_validation_results[0].id,
            "message": "AESTDY and DOMAIN are equal to test",
            "executability": "Fully Executable",
            "dataset": None,
            "USUBJID": "CDISC003",
            "row": 9,
            "SEQ": 10,
            "variables": ["AESTDY", "DOMAIN"],
            "values": ["test", "test"],
        },
        {
            "core_id": mock_validation_results[1].id,
            "message": "TTVARs are wrong",
            "executability": "Partially Executable",
            "dataset": None,
            "USUBJID": "CDISC002",
            "row": 1,
            "SEQ": 2,
            "variables": ["TTVAR1", "TTVAR2"],
            "values": ["test", "test"],
        },
    ]
    errors = sorted(errors, key=lambda x: (x["core_id"], x["dataset"]))
    assert len(errors) == len(detailed_data)
    for i, error in enumerate(errors):
        assert error == detailed_data[i]


def test_get_summary_data(mock_validation_results):
    report = SDTMReportData(
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
            "dataset": None,
            "core_id": mock_validation_results[0].id,
            "message": "AESTDY and DOMAIN are equal to test",
            "issues": 2,
        },
        {
            "dataset": None,
            "core_id": mock_validation_results[1].id,
            "message": "TTVARs are wrong",
            "issues": 1,
        },
    ]
    errors = sorted(errors, key=lambda x: (x["dataset"], x["core_id"]))
    assert len(errors) == len(summary_data)
    for i, error in enumerate(errors):
        assert error == summary_data[i]
