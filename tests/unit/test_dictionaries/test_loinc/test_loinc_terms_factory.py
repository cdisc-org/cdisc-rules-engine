import os
from unittest.mock import MagicMock

from cdisc_rules_engine.models.dictionaries import DictionaryTypes, AbstractTermsFactory
from cdisc_rules_engine.services.data_services.local_data_service import (
    LocalDataService,
)

dictionary_path = f"{os.path.dirname(__file__)}/../../../resources/dictionaries/loinc"


def test_install():
    storage_service = LocalDataService.get_instance(cache_service=MagicMock())
    factory = AbstractTermsFactory(storage_service).get_service(
        DictionaryTypes.LOINC.value
    )
    dictionary = factory.install_terms(dictionary_path)
    assert dictionary.version == "2.74"
    items = dictionary.items()
    assert len(items) == 3
    expected = ["100000-9", "100001-7", "100002-5"]
    for i, code in enumerate(dictionary):
        assert code == expected[i]
