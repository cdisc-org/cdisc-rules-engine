from unittest.mock import MagicMock
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.services.reporting.ddf_report_data import DDFReportData


def test_get_rules_report_data(mock_validation_results):
    report = DDFReportData(
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
                "status": ExecutionStatus.SUCCESS.value.upper(),
            }
        )
    expected_reports = sorted(expected_reports, key=lambda x: x["core_id"])
    assert len(report_data) == len(expected_reports)
    for i, _ in enumerate(report_data):
        assert report_data[i] == expected_reports[i]


def test_get_detailed_data(mock_validation_results):
    report = DDFReportData(
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
            "variables": ["AESTDY", "DOMAIN"],
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
            "variables": ["AESTDY", "DOMAIN"],
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
            "variables": ["TTVAR1", "TTVAR2"],
            "values": ["test", "test"],
        },
    ]
    errors = sorted(errors, key=lambda x: (x["core_id"], x["entity"]))
    assert len(errors) == len(detailed_data)
    for i, error in enumerate(errors):
        assert error == detailed_data[i]


def test_get_summary_data(mock_validation_results):
    report = DDFReportData(
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
            "message": "AESTDY and DOMAIN are equal to test",
            "issues": 2,
        },
        {
            "entity": "",
            "core_id": mock_validation_results[1].id,
            "message": "TTVARs are wrong",
            "issues": 1,
        },
    ]
    errors = sorted(errors, key=lambda x: (x["entity"], x["core_id"]))
    assert len(errors) == len(summary_data)
    for i, error in enumerate(errors):
        assert error == summary_data[i]
