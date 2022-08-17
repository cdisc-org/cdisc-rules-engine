import os
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.utilities.excel_report import ExcelReport

test_report_template: str = f"{os.path.dirname(__file__)}/../../cdisc_rules_engine/resources/templates/report-template.xlsx"

mock_validation_results = [
    RuleValidationResult(
        rule={"core_id": "CORE1", "severity": "Error", "message": "TEST RULE 1"},
        results=[
            {
                "domain": "AE",
                "variables": ["AESTDY", "DOMAIN"],
                "executionStatus": ExecutionStatus.SUCCESS.value,
                "errors": [
                    {
                        "row": 1,
                        "value": {"AESTDY": "test", "DOMAIN": "test"},
                        "uSubjId": "CDISC002",
                        "seq": 2,
                    },
                    {
                        "row": 9,
                        "value": {"AESTDY": "test", "DOMAIN": "test"},
                        "uSubjId": "CDISC003",
                        "seq": 10,
                    },
                ],
                "message": "AESTDY and DOMAIN are equal to test",
            }
        ],
    ),
    RuleValidationResult(
        rule={"core_id": "CORE2", "severity": "Warning", "message": "TEST RULE 2"},
        results=[
            {
                "domain": "TT",
                "variables": ["TTVAR1", "TTVAR2"],
                "executionStatus": ExecutionStatus.SUCCESS.value,
                "errors": [
                    {
                        "row": 1,
                        "value": {"TTVAR1": "test", "TTVAR2": "test"},
                        "uSubjId": "CDISC002",
                        "seq": 2,
                    }
                ],
                "message": "TTVARs are wrong",
            }
        ],
    ),
]


def test_get_rules_report_data():
    with open(test_report_template, "rb") as f:
        report: ExcelReport = ExcelReport("test", mock_validation_results, 10.1, {}, f)
        report_data = report.get_rules_report_data()
        expected_reports = []
        for result in mock_validation_results:
            expected_reports.append(
                [result.id, "1", result.message, ExecutionStatus.SUCCESS.value.upper()]
            )
        expected_reports = sorted(expected_reports, key=lambda x: x[0])
        assert len(report_data) == len(expected_reports)
        for i, _ in enumerate(report_data):
            assert report_data[i] == expected_reports[i]


def test_get_detailed_data():
    with open(test_report_template, "rb") as f:
        report: ExcelReport = ExcelReport("test", mock_validation_results, 10.1, {}, f)
        detailed_data = report.get_detailed_data()
        errors = [
            [
                mock_validation_results[0].id,
                "AESTDY and DOMAIN are equal to test",
                "Error",
                "AE",
                "CDISC002",
                1,
                2,
                "AESTDY, DOMAIN",
                "test, test",
            ],
            [
                mock_validation_results[0].id,
                "AESTDY and DOMAIN are equal to test",
                "Error",
                "AE",
                "CDISC003",
                9,
                10,
                "AESTDY, DOMAIN",
                "test, test",
            ],
            [
                mock_validation_results[1].id,
                "TTVARs are wrong",
                "Warning",
                "TT",
                "CDISC002",
                1,
                2,
                "TTVAR1, TTVAR2",
                "test, test",
            ],
        ]
        errors = sorted(errors, key=lambda x: (x[0], x[2]))
        assert len(errors) == len(detailed_data)
        for i, error in enumerate(errors):
            assert error == detailed_data[i]


def test_get_summary_data():
    with open(test_report_template, "rb") as f:
        report: ExcelReport = ExcelReport("test", mock_validation_results, 10.1, {}, f)
        summary_data = report.get_summary_data()
        errors = [
            [
                "AE",
                mock_validation_results[0].id,
                "AESTDY and DOMAIN are equal to test",
                "Error",
                2,
            ],
            ["TT", mock_validation_results[1].id, "TTVARs are wrong", "Warning", 1],
        ]
        errors = sorted(errors, key=lambda x: (x[0], x[1]))
        assert len(errors) == len(summary_data)
        for i, error in enumerate(errors):
            assert error == summary_data[i]


def test_get_export():
    with open(test_report_template, "rb") as f:
        report: ExcelReport = ExcelReport("test", mock_validation_results, 10.1, {}, f)
        cdiscCt = ["sdtmct-03-2021"]
        wb = report.get_export(
            define_version="2.1", cdiscCt=cdiscCt, standard="sdtmig", version="3.4"
        )
        assert wb["Conformance Details"]["B2"].value == "test"
        assert wb["Conformance Details"]["B4"].value == "10.1 seconds"
        assert wb["Conformance Details"]["B8"].value == "SDTMIG"
        assert wb["Conformance Details"]["B9"].value == f"V3.4"
        assert wb["Conformance Details"]["B10"].value == ", ".join(cdiscCt)
        assert wb["Conformance Details"]["B11"].value == "2.1"
