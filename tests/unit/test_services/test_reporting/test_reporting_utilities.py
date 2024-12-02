import os

from cdisc_rules_engine.utilities.reporting_utilities import (
    get_define_version,
)

path_to_dataset = (
    f"{os.path.dirname(__file__)}/"
    f"../../../resources/report_test_data/test_dataset.xpt"
)


def test_get_version_and_CT_from_define():
    version = get_define_version([path_to_dataset])
    assert version[0] == "2.1.0"
    assert version[1] == ["sdtmct-2020-12-18"]
