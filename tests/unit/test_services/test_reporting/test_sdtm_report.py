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


def test_get_detailed_data_with_compare_groups():
    """Test detailed data generation with comparison groups."""
    from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult

    rule = {
        "core_id": "CORE-000334",
        "actions": [
            {
                "name": "generate_targeted_error_objects",
                "params": {"message": "Test comparison"},
            }
        ],
        "executability": "Fully Executable",
        "authorities": [],
    }
    validation_result = RuleValidationResult(
        rule,
        [
            {
                "executionStatus": "success",
                "variables": ["$dataset_variables", "$expected_variables"],
                "compare_groups": [["$dataset_variables", "$expected_variables"]],
                "errors": [
                    {
                        "dataset": "AE",
                        "USUBJID": "001",
                        "row": 1,
                        "SEQ": 1,
                        "value": {
                            "$dataset_variables": ["VAR1", "VAR2"],
                            "$expected_variables": ["VAR1", "VAR2", "VAR3"],
                        },
                    }
                ],
                "message": "Test comparison",
            }
        ],
    )

    report = SDTMReportData(
        [],
        ["test"],
        [validation_result],
        10.1,
        MagicMock(define_xml_path=None, max_errors_per_rule=(None, False)),
    )
    detailed_data = report.get_detailed_data()

    assert len(detailed_data) == 1
    error = detailed_data[0]
    assert error["core_id"] == "CORE-000334"
    assert error["message"] == "Test comparison"
    # Variables should be aligned (comma-separated list of comparison group vars)
    assert error["variables"] == ["$dataset_variables, $expected_variables"]
    # Values should contain comparison result string
    assert len(error["values"]) == 1
    assert "Missing in" in error["values"][0] or "Extra in" in error["values"][0]


def test_get_detailed_data_with_multiple_compare_groups():
    """Test detailed data with multiple comparison groups."""
    from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult

    rule = {
        "core_id": "CORE-000335",
        "actions": [
            {"name": "generate_targeted_error_objects", "params": {"message": "Test"}}
        ],
        "executability": "Fully Executable",
        "authorities": [],
    }
    validation_result = RuleValidationResult(
        rule,
        [
            {
                "executionStatus": "success",
                "variables": ["$VAR1", "$VAR2", "$VAR3", "$VAR4"],
                "compare_groups": [["$VAR1", "$VAR2"], ["$VAR3", "$VAR4"]],
                "errors": [
                    {
                        "dataset": "AE",
                        "USUBJID": "001",
                        "row": 1,
                        "SEQ": 1,
                        "value": {
                            "$VAR1": ["A", "B"],
                            "$VAR2": ["A"],
                            "$VAR3": [1, 2],
                            "$VAR4": [1, 2, 3],
                        },
                    }
                ],
                "message": "Test",
            }
        ],
    )

    report = SDTMReportData(
        [],
        ["test"],
        [validation_result],
        10.1,
        MagicMock(define_xml_path=None, max_errors_per_rule=(None, False)),
    )
    detailed_data = report.get_detailed_data()

    assert len(detailed_data) == 1
    error = detailed_data[0]
    # Should have 2 values (one per comparison group)
    assert len(error["values"]) == 2
    # Variables should be aligned to match values count
    assert len(error["variables"]) == 2


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
