import pytest

from cdisc_rules_engine.models.dictionaries.whodrug import (
    DrugDictionary,
    WhodrugRecordTypes,
)


@pytest.mark.parametrize(
    "line, expected_code, expected_drug_name",
    [
        ("000001010016N  001      01 854METHYLDOPA      ", "000001", "METHYLDOPA"),
        ("000001010909T05237      01 061CARDIODOPA        ", "000001", "CARDIODOPA"),
    ],
)
def test_drug_dictionary_model_from_txt_line(
    line: str,
    expected_code: str,
    expected_drug_name: str,
):
    """
    Unit test for DrugDictionary.from_txt_line method.
    """
    drug_dictionary_model = DrugDictionary.from_txt_line(line)
    assert drug_dictionary_model.code == expected_code
    assert drug_dictionary_model.drugName == expected_drug_name
    assert drug_dictionary_model.type == WhodrugRecordTypes.DRUG_DICT.value
