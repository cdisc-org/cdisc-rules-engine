import pytest

from cdisc_rules_engine.models.dictionaries.whodrug import (
    AtcClassification,
    WhodrugRecordTypes,
)


@pytest.mark.parametrize(
    "line, expected_parent_code, expected_code",
    [
        (
            "000001010016C02AB  111*",
            "000001",
            "C02AB",
        ),
        (
            "000004020012D04A   203 ",
            "000004",
            "D04A",
        ),
    ],
)
def test_atc_classification_model_from_txt_line(
    line: str,
    expected_parent_code: str,
    expected_code: str,
):
    """
    Unit test for AtcClassification.from_txt_line method.
    """
    atc_classification_model = AtcClassification.from_txt_line(line)
    assert atc_classification_model.parentCode == expected_parent_code
    assert atc_classification_model.code == expected_code
    assert atc_classification_model.type == WhodrugRecordTypes.ATC_CLASSIFICATION.value
