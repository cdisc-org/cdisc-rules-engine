import pytest

from cdisc_rules_engine.models.dictionaries.whodrug import AtcText, WhodrugRecordTypes


@pytest.mark.parametrize(
    "line, expected_parent_code, expected_level, expected_text",
    [
        (
            "A02AD  4COMBINATIONS AND COMPLEXES OF ALUMINIUM, CALCIUM AND MAGNESIUM COMPOUNDS",
            "A02AD",
            4,
            "COMBINATIONS AND COMPLEXES OF ALUMINIUM, CALCIUM AND MAGNESIUM COMPOUNDS",
        ),
        (
            "A      1ALIMENTARY TRACT AND METABOLISM    ",
            "A",
            1,
            "ALIMENTARY TRACT AND METABOLISM",
        ),
    ],
)
def test_atc_text_model_from_txt_line(
    line: str,
    expected_parent_code: str,
    expected_level: int,
    expected_text: str,
):
    """
    Unit test for AtcText.from_txt_line method.
    """
    atc_text_model = AtcText.from_txt_line(line)
    assert atc_text_model.parentCode == expected_parent_code
    assert atc_text_model.level == expected_level
    assert atc_text_model.text == expected_text
    assert atc_text_model.type == WhodrugRecordTypes.ATC_TEXT.value
