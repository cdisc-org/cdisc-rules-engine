from unittest.mock import MagicMock, patch
from cdisc_rules_engine.services.reporting import sdtm_report_data
from cdisc_rules_engine.services.reporting.sdtm_report_data import SDTMReportData
from version import __version__

from cdisc_rules_engine.services.reporting.json_report import JsonReport


@patch.object(
    sdtm_report_data.SDTMReportData,
    "get_define_version",
    return_value="2.1",
)
def test_get_export(_, mock_validation_results):
    mock_args = MagicMock()
    mock_args.max_errors_per_rule = (None, False)
    mock_args.controlled_terminology_package = ["sdtmct-03-2021"]
    mock_args.standard = "sdtmig"
    mock_args.version = "3.4"
    mock_args.dictionary_versions = {}
    mock_args.whodrug = "test"
    report_standard = SDTMReportData(
        [], ["test"], mock_validation_results, 10.1, mock_args
    )
    report: JsonReport = JsonReport(report_standard, mock_args, None)
    export = report.get_export(
        raw_report=False,
    )
    assert export["Conformance_Details"]["CORE_Engine_Version"] == __version__
    assert export["Conformance_Details"]["Total_Runtime"] == "10.1 seconds"
    assert export["Conformance_Details"]["Standard"] == "SDTMIG"
    assert export["Conformance_Details"]["Version"] == "V3.4"
    assert export["Conformance_Details"]["CT_Version"] == "sdtmct-03-2021"
    assert export["Conformance_Details"]["Define_XML_Version"] == "2.1"
    assert export["Conformance_Details"]["Issue_Limit_Per_Rule"] == "None"
    assert export["Conformance_Details"]["Issue_Limit_Per_Dataset"] == "None"
    assert export["Conformance_Details"]["Issue_Limit_Per_Sheet"] is None
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
