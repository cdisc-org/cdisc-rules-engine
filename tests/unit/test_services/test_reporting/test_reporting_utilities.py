import os

from cdisc_rules_engine.services.reporting.sdtm_report_data import SDTMReportData

path_to_dataset = (
    f"{os.path.dirname(__file__)}/"
    f"../../../resources/report_test_data/test_dataset.xpt"
)


def test_get_version_from_define():
    version = SDTMReportData.get_define_version([path_to_dataset])
    assert version == "2.1.0"
