from cdisc_rules_engine.models.dictionaries.whodrug import (
    DrugDictionary,
    AtcText,
    AtcClassification,
)
from cdisc_rules_engine.serializers import (
    DrugDictionarySerializer,
    AtcTextSerializer,
    AtcClassificationSerializer,
)


def test_serializer_data():
    dd = DrugDictionary.from_txt_line("000001010016N  001      01 854METHYLDOPA      ")
    dd_data = DrugDictionarySerializer(dd).data
    assert dd_data == {"code": dd.code, "type": dd.type, "drugName": dd.drugName}
    atc = AtcText.from_txt_line(
        "A02AD  4COMBINATIONS AND COMPLEXES OF ALUMINIUM,"
        " CALCIUM AND MAGNESIUM COMPOUNDS"
    )
    atc_data = AtcTextSerializer(atc).data
    assert atc_data == {
        "code": atc.code,
        "parentCode": atc.parentCode,
        "level": atc.level,
        "text": atc.text,
        "type": atc.type,
    }
    atc_c = AtcClassification.from_txt_line("000004020012D04A   203 ")
    atc_c_data = AtcClassificationSerializer(atc_c).data
    assert atc_c_data == {
        "code": atc_c.code,
        "type": atc_c.type,
        "parentCode": atc_c.parentCode,
    }
