from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.utilities.json_report import JsonReport

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
    report: JsonReport = JsonReport("test", mock_validation_results, 10.1, {})
    report_data = report.get_rules_report_data()
    expected_reports = []
    for result in mock_validation_results:
        expected_reports.append(
            {
                "rule_id": result.id,
                "version": "1",
                "message": result.message,
                "status": ExecutionStatus.SUCCESS.value.upper(),
            }
        )
    expected_reports = sorted(expected_reports, key=lambda x: x["rule_id"])
    assert len(report_data) == len(expected_reports)
    for i, _ in enumerate(report_data):
        assert report_data[i] == expected_reports[i]


def test_get_detailed_data():
    report: JsonReport = JsonReport("test", mock_validation_results, 10.1, {})
    detailed_data = report.get_detailed_data()
    errors = [
        {
            "rule_id": mock_validation_results[0].id,
            "message": "AESTDY and DOMAIN are equal to test",
            "severity": "Error",
            "dataset": "AE",
            "uSubjId": "CDISC002",
            "row": 1,
            "seq": 2,
            "variables": ["AESTDY", "DOMAIN"],
            "values": ["test", "test"],
        },
        {
            "rule_id": mock_validation_results[0].id,
            "message": "AESTDY and DOMAIN are equal to test",
            "severity": "Error",
            "dataset": "AE",
            "uSubjId": "CDISC003",
            "row": 9,
            "seq": 10,
            "variables": ["AESTDY", "DOMAIN"],
            "values": ["test", "test"],
        },
        {
            "rule_id": mock_validation_results[1].id,
            "message": "TTVARs are wrong",
            "severity": "Warning",
            "dataset": "TT",
            "uSubjId": "CDISC002",
            "row": 1,
            "seq": 2,
            "variables": ["TTVAR1", "TTVAR2"],
            "values": ["test", "test"],
        },
    ]
    errors = sorted(errors, key=lambda x: (x["rule_id"], x["dataset"]))
    assert len(errors) == len(detailed_data)
    for i, error in enumerate(errors):
        assert error == detailed_data[i]


def test_get_summary_data():
    report: JsonReport = JsonReport("test", mock_validation_results, 10.1, {})
    summary_data = report.get_summary_data()
    errors = [
        {
            "dataset": "AE",
            "rule_id": mock_validation_results[0].id,
            "message": "AESTDY and DOMAIN are equal to test",
            "severity": "Error",
            "issues": 2,
        },
        {
            "dataset": "TT",
            "rule_id": mock_validation_results[1].id,
            "message": "TTVARs are wrong",
            "severity": "Warning",
            "issues": 1,
        },
    ]
    errors = sorted(errors, key=lambda x: (x["dataset"], x["rule_id"]))
    assert len(errors) == len(summary_data)
    for i, error in enumerate(errors):
        assert error == summary_data[i]


def test_get_export():
    report: JsonReport = JsonReport("test", mock_validation_results, 10.1, {})
    cdiscCt = ["sdtmct-03-2021"]
    export = report.get_export(
        define_version="2.1",
        cdiscCt=cdiscCt,
        standard="sdtmig",
        version="3.4",
        raw_report=False,
    )
    assert export["conformance_details"]["data_path"] == "test"
    assert export["conformance_details"]["runtime"] == 10.1
    assert export["bundle_details"]["standard"] == "SDTMIG"
    assert export["bundle_details"]["version"] == "3.4"
    assert export["bundle_details"]["cdisc_ct"] == cdiscCt
    assert export["bundle_details"]["define_version"] == "2.1"
