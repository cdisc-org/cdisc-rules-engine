from unittest.mock import MagicMock

from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.services.reporting.json_report import JsonReport

mock_validation_results = [
    RuleValidationResult(
        rule={
            "core_id": "CORE1",
            "executability": "Fully Executable",
            "actions": [{"params": {"message": "TEST RULE 1"}}],
            "authorities": [
                {
                    "Organization": "CDISC",
                    "Standards": [
                        {
                            "References": [
                                {"Rule_Identifier": {"Id": "CDISCRuleID4"}},
                                {"Rule_Identifier": {"Id": "CDISCRuleID3"}},
                            ]
                        },
                        {
                            "References": [
                                {"Rule_Identifier": {"Id": "CDISCRuleID2"}},
                                {"Rule_Identifier": {"Id": "CDISCRuleID1"}},
                            ]
                        },
                    ],
                },
                {
                    "Organization": "FDA",
                    "Standards": [
                        {
                            "References": [
                                {"Rule_Identifier": {"Id": "FDARuleID1"}},
                                {"Rule_Identifier": {"Id": "FDARuleID2"}},
                            ]
                        }
                    ],
                },
                {
                    "Organization": "PMDA",
                    "Standards": [
                        {
                            "References": [
                                {"Rule_Identifier": {"Id": "PMDARuleID1"}},
                                {"Rule_Identifier": {"Id": "PMDARuleID2"}},
                            ]
                        }
                    ],
                },
            ],
        },
        results=[
            {
                "domain": "AE",
                "variables": ["AESTDY", "DOMAIN"],
                "executionStatus": ExecutionStatus.SUCCESS.value,
                "errors": [
                    {
                        "row": 1,
                        "value": {"AESTDY": "test", "DOMAIN": "test"},
                        "USUBJID": "CDISC002",
                        "SEQ": 2,
                    },
                    {
                        "row": 9,
                        "value": {"AESTDY": "test", "DOMAIN": "test"},
                        "USUBJID": "CDISC003",
                        "SEQ": 10,
                    },
                ],
                "message": "AESTDY and DOMAIN are equal to test",
            }
        ],
    ),
    RuleValidationResult(
        rule={
            "core_id": "CORE2",
            "executability": "Partially Executable",
            "actions": [{"params": {"message": "TEST RULE 2"}}],
            "authorities": [
                {
                    "Organization": "CDISC",
                    "Standards": [
                        {
                            "References": [
                                {"Rule_Identifier": {"Id": "CDISCRuleID4"}},
                                {"Rule_Identifier": {"Id": "CDISCRuleID3"}},
                            ]
                        },
                        {
                            "References": [
                                {"Rule_Identifier": {"Id": "CDISCRuleID2"}},
                                {"Rule_Identifier": {"Id": "CDISCRuleID1"}},
                            ]
                        },
                    ],
                },
                {
                    "Organization": "FDA",
                    "Standards": [
                        {
                            "References": [
                                {"Rule_Identifier": {"Id": "FDARuleID1"}},
                                {"Rule_Identifier": {"Id": "FDARuleID2"}},
                            ]
                        }
                    ],
                },
                {
                    "Organization": "PMDA",
                    "Standards": [
                        {
                            "References": [
                                {"Rule_Identifier": {"Id": "PMDARuleID1"}},
                                {"Rule_Identifier": {"Id": "PMDARuleID2"}},
                            ]
                        }
                    ],
                },
            ],
        },
        results=[
            {
                "domain": "TT",
                "variables": ["TTVAR1", "TTVAR2"],
                "executionStatus": ExecutionStatus.SUCCESS.value,
                "errors": [
                    {
                        "row": 1,
                        "value": {"TTVAR1": "test", "TTVAR2": "test"},
                        "USUBJID": "CDISC002",
                        "SEQ": 2,
                    }
                ],
                "message": "TTVARs are wrong",
            }
        ],
    ),
]


def test_get_rules_report_data():
    report: JsonReport = JsonReport(
        [], "test", mock_validation_results, 10.1, MagicMock()
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
                "pmda_rule_id": result.pmda_rule_id,
                "message": result.message,
                "status": ExecutionStatus.SUCCESS.value.upper(),
            }
        )
    expected_reports = sorted(expected_reports, key=lambda x: x["core_id"])
    assert len(report_data) == len(expected_reports)
    for i, _ in enumerate(report_data):
        assert report_data[i] == expected_reports[i]


def test_get_detailed_data():
    report: JsonReport = JsonReport(
        [], "test", mock_validation_results, 10.1, MagicMock()
    )
    detailed_data = report.get_detailed_data()
    errors = [
        {
            "core_id": mock_validation_results[0].id,
            "message": "AESTDY and DOMAIN are equal to test",
            "executability": "Fully Executable",
            "dataset": "AE",
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
            "dataset": "AE",
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
            "dataset": "TT",
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


def test_get_summary_data():
    report: JsonReport = JsonReport(
        [], "test", mock_validation_results, 10.1, MagicMock()
    )
    summary_data = report.get_summary_data()
    errors = [
        {
            "dataset": "AE",
            "core_id": mock_validation_results[0].id,
            "message": "AESTDY and DOMAIN are equal to test",
            "issues": 2,
        },
        {
            "dataset": "TT",
            "core_id": mock_validation_results[1].id,
            "message": "TTVARs are wrong",
            "issues": 1,
        },
    ]
    errors = sorted(errors, key=lambda x: (x["dataset"], x["core_id"]))
    assert len(errors) == len(summary_data)
    for i, error in enumerate(errors):
        assert error == summary_data[i]


def test_get_export():
    report: JsonReport = JsonReport(
        [], "test", mock_validation_results, 10.1, MagicMock()
    )
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
