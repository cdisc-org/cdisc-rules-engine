import os
from unittest.mock import MagicMock, patch

from cdisc_rules_engine.enums.default_file_paths import DefaultFilePaths
from cdisc_rules_engine.services.reporting import sdtm_report_data
from cdisc_rules_engine.services.reporting.excel_report import ExcelReport
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from pathlib import Path
from cdisc_rules_engine.services.reporting.sdtm_report_data import SDTMReportData
from version import __version__

test_report_template: str = (
    f"{os.path.dirname(__file__)}/../../../../{DefaultFilePaths.EXCEL_TEMPLATE_FILE.value}"
)


@patch.object(
    sdtm_report_data.SDTMReportData,
    "get_define_version",
    return_value="2.1",
)
def test_get_export(mock_validation_results):
    with open(test_report_template, "rb") as f:
        mock_args = MagicMock()
        mock_args.meddra = "test"
        mock_args.whodrug = "test"
        mock_args.max_report_rows = None
        mock_args.max_errors_per_rule = (10, True)
        mock_args.controlled_terminology_package = ["sdtmct-03-2021"]
        mock_args.standard = "sdtmig"
        mock_args.substandard = None
        mock_args.version = "3.4"
        mock_args.use_case = None
        mock_args.dictionary_versions = {}
        datasets = [
            SDTMDatasetMetadata(
                **{
                    "name": "test",
                    "filename": "test.xpt",
                    "label": "Test Data",
                    "full_path": str(Path("tests/unit/text.xpt")),
                    "modification_date": "2022-04-19T16:17:45",
                    "file_size": 20000,
                    "record_count": 700,
                }
            )
        ]
        report_standard = SDTMReportData(
            datasets, ["test"], mock_validation_results, 10.1, mock_args
        )
        report: ExcelReport = ExcelReport(report_standard, mock_args, f)
        wb = report.get_export()
        assert wb["Conformance Details"]["B3"].value == "10.1 seconds"
        assert wb["Conformance Details"]["B4"].value == __version__
        assert wb["Conformance Details"]["B5"].value == 10
        assert wb["Conformance Details"]["B6"].value == "True"
        assert wb["Conformance Details"]["B7"].value == "10000"
        assert wb["Conformance Details"]["B9"].value == "SDTMIG"
        assert wb["Conformance Details"]["B10"].value == "NAP"
        assert wb["Conformance Details"]["B11"].value == "V3.4"
        assert wb["Conformance Details"]["B12"].value == "NAP"
        assert wb["Conformance Details"]["B13"].value == ", ".join(
            mock_args.controlled_terminology_package
        )
        assert wb["Conformance Details"]["B14"].value == "2.1"

        # Check dataset details tab
        assert wb["Dataset Details"]["A2"].value == "test"  # filename
        assert wb["Dataset Details"]["B2"].value == "Test Data"  # label
        assert wb["Dataset Details"]["C2"].value == str(
            Path("tests/unit/text.xpt").parent
        )  # Location
        assert (
            wb["Dataset Details"]["D2"].value == "2022-04-19T16:17:45"
        )  # Modified Time Stamp
        assert wb["Dataset Details"]["E2"].value == 20  # Size in kb
        assert wb["Dataset Details"]["F2"].value == 700  # Number of records
