import os
from unittest.mock import MagicMock

from cdisc_rules_engine.models.dictionaries import DictionaryTypes, AbstractTermsFactory
from cdisc_rules_engine.services.data_services.local_data_service import (
    LocalDataService,
)

dictionary_path = f"{os.path.dirname(__file__)}/../../../resources/dictionaries/unii"


def test_install():
    storage_service = LocalDataService.get_instance(
        cache_service=MagicMock(), config=MagicMock()
    )
    factory = AbstractTermsFactory(storage_service).get_service(
        DictionaryTypes.UNII.value
    )
    dictionary = factory.install_terms(dictionary_path)
    assert dictionary.version == "27Sep2024"
    assert len(dictionary.items()) == 9
    names = [term.display_name for term in dictionary.values()]
    codes = [term.unii for term in dictionary.values()]
    assert names == [
        "CEROUS SALICYLATE",
        "DI(DEHYDROABIETYL)AMINE ACETATE",
        "SYMPHYOTRICHUM OBLONGIFOLIUM WHOLE",
        "ADECATUMUMAB",
        "GERMANIUM",
        "SILVERGRAY ROCKFISH",
        "PLANTAGO AUSTRALIS WHOLE",
        "(5-((2,4-DIAMINOPYRIMIDIN-5-YL)METHYL)-2,3-DIMETHOXYPHENYL) HYPOBROMITE",
        "DIHEPTYLUNDECYL ADIPATE",
    ]
    assert codes == [
        "0001H6R5H1",
        "000360VJE1",
        "0005633KTU",
        "000705ZASD",
        "00072J7XWS",
        "00083HM9BY",
        "0008D0X3XI",
        "0009YL8Y42",
        "000BPI4XCV",
    ]
