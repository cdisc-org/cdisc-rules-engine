from cdisc_rules_engine.utilities.reporting_utilities import (
    get_define_version,
)

path_to_dataset = "../../../resources/report_test_data/test_dataset.xpt"


def test_get_version_from_define():
    version = get_define_version([path_to_dataset])
    assert version == "2.1.0"
