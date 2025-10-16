import os
from unittest.mock import MagicMock

from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.services.reporting.excel_report import ExcelReport
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from pathlib import Path
from version import __version__

test_report_template: str = (
    f"{os.path.dirname(__file__)}/../../../../resources/templates/report-template.xlsx"
)

mock_validation_results = [
    RuleValidationResult(
        rule={
            "core_id": "CORE1",
            "executability": "Partially Executable",
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
            "executability": "Fully Executable",
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
    with open(test_report_template, "rb") as f:
        mock_args = MagicMock()
        mock_args.max_report_rows = None
        mock_args.max_errors_per_rule = (None, False)
        report: ExcelReport = ExcelReport(
            [], "test", mock_validation_results, 10.1, mock_args, f
        )
        report_data = report.get_rules_report_data()
        expected_reports = []
        for result in mock_validation_results:
            expected_reports.append(
                [
                    result.id,
                    "1",
                    result.cdisc_rule_id,
                    result.fda_rule_id,
                    result.message,
                    ExecutionStatus.SUCCESS.value.upper(),
                ]
            )
        expected_reports = sorted(expected_reports, key=lambda x: x[0])
        assert len(report_data) == len(expected_reports)
        for i, _ in enumerate(report_data):
            assert report_data[i] == expected_reports[i]


def test_get_detailed_data(excel=True):
    with open(test_report_template, "rb") as f:
        mock_args = MagicMock()
        mock_args.max_report_rows = None
        mock_args.max_errors_per_rule = (None, False)
        report: ExcelReport = ExcelReport(
            [], "test", mock_validation_results, 10.1, mock_args, f
        )
        detailed_data = report.get_detailed_data(excel=True)
        errors = [
            [
                mock_validation_results[0].id,
                "AESTDY and DOMAIN are equal to test",
                "Partially Executable",
                None,
                "CDISC002",
                1,
                2,
                "AESTDY, DOMAIN",
                "test, test",
            ],
            [
                mock_validation_results[0].id,
                "AESTDY and DOMAIN are equal to test",
                "Partially Executable",
                None,
                "CDISC003",
                9,
                10,
                "AESTDY, DOMAIN",
                "test, test",
            ],
            [
                mock_validation_results[1].id,
                "TTVARs are wrong",
                "Fully Executable",
                None,
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
        mock_args = MagicMock()
        mock_args.max_report_rows = None
        mock_args.max_errors_per_rule = (None, False)
        report: ExcelReport = ExcelReport(
            [], "test", mock_validation_results, 10.1, mock_args, f
        )
        summary_data = report.get_summary_data()
        errors = [
            [
                None,
                mock_validation_results[0].id,
                "AESTDY and DOMAIN are equal to test",
                2,
            ],
            [None, mock_validation_results[1].id, "TTVARs are wrong", 1],
        ]
        errors = sorted(errors, key=lambda x: (x[0], x[1]))
        assert len(errors) == len(summary_data)
        for i, error in enumerate(errors):
            assert error == summary_data[i]


def test_get_export():
    with open(test_report_template, "rb") as f:
        mock_args = MagicMock()
        mock_args.meddra = "test"
        mock_args.whodrug = "test"
        mock_args.max_report_rows = None
        mock_args.max_errors_per_rule = (None, False)

        datasets = [
            SDTMDatasetMetadata(
                **{
                    "filename": "test.xpt",
                    "label": "Test Data",
                    "full_path": str(Path("tests/unit/text.xpt")),
                    "modification_date": "2022-04-19T16:17:45",
                    "file_size": 20000,
                    "record_count": 700,
                }
            )
        ]
        report: ExcelReport = ExcelReport(
            datasets, ["test"], mock_validation_results, 10.1, mock_args, f
        )
        cdiscCt = ["sdtmct-03-2021"]
        wb = report.get_export(
            define_version="2.1",
            cdiscCt=cdiscCt,
            standard="sdtmig",
            version="3.4",
            dictionary_versions={},
        )
        assert wb["Conformance Details"]["B3"].value == "10.1 seconds"
        assert wb["Conformance Details"]["B4"].value == __version__
        assert wb["Conformance Details"]["B9"].value == "SDTMIG"
        assert wb["Conformance Details"]["B10"].value == "NAP"
        assert wb["Conformance Details"]["B11"].value == "V3.4"
        assert wb["Conformance Details"]["B12"].value == ", ".join(cdiscCt)
        assert wb["Conformance Details"]["B13"].value == "2.1"

        # Check dataset details tab
        assert wb["Dataset Details"]["A2"].value == "test.xpt"  # filename
        assert wb["Dataset Details"]["B2"].value == "Test Data"  # label
        assert wb["Dataset Details"]["C2"].value == str(
            Path("tests/unit/text.xpt").parent
        )  # Location
        assert (
            wb["Dataset Details"]["D2"].value == "2022-04-19T16:17:45"
        )  # Modified Time Stamp
        assert wb["Dataset Details"]["E2"].value == 20  # Size in kb
        assert wb["Dataset Details"]["F2"].value == 700  # Number of records
