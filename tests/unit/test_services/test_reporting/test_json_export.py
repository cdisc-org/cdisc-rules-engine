from unittest.mock import MagicMock
from version import __version__

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
    mock_args = MagicMock()
    mock_args.max_errors_per_rule = (None, False)
    report: JsonReport = JsonReport(
        [], "test", mock_validation_results, 10.1, mock_args
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


def test_get_detailed_data():
    mock_args = MagicMock()
    mock_args.max_errors_per_rule = (None, False)
    report: JsonReport = JsonReport(
        [], "test", mock_validation_results, 10.1, mock_args
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


def test_get_summary_data():
    mock_args = MagicMock()
    mock_args.max_errors_per_rule = (None, False)
    report: JsonReport = JsonReport(
        [], "test", mock_validation_results, 10.1, mock_args
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


def test_get_export():
    mock_args = MagicMock()
    mock_args.max_errors_per_rule = (None, False)
    report: JsonReport = JsonReport(
        [], "test", mock_validation_results, 10.1, mock_args
    )
    cdiscCt = ["sdtmct-03-2021"]
    export = report.get_export(
        define_version="2.1",
        cdiscCt=cdiscCt,
        standard="sdtmig",
        version="3.4",
        raw_report=False,
        dictionary_versions={},
    )
    assert export["Conformance_Details"]["CORE_Engine_Version"] == __version__
    assert export["Conformance_Details"]["Total_Runtime"] == "10.1 seconds"
    assert export["Conformance_Details"]["Standard"] == "SDTMIG"
    assert export["Conformance_Details"]["Version"] == "V3.4"
    assert export["Conformance_Details"]["CT_Version"] == "sdtmct-03-2021"
    assert export["Conformance_Details"]["Define_XML_Version"] == "2.1"
    assert export["Conformance_Details"]["Issue_Limit_Per_Rule"] is None
    assert export["Conformance_Details"]["Issue_Limit_Per_Dataset"] is False
    assert "Dataset_Details" in export
    assert isinstance(export["Dataset_Details"], list)

    assert "Issue_Summary" in export
    assert isinstance(export["Issue_Summary"], list)
    assert "Issue_Details" in export
    assert isinstance(export["Issue_Details"], list)
    assert "Rules_Report" in export
    assert isinstance(export["Rules_Report"], list)
    assert "Issue_Summary" in export
    assert len(export["Issue_Summary"]) > 0
    assert len(export["Issue_Details"]) > 0
    assert len(export["Rules_Report"]) > 0
