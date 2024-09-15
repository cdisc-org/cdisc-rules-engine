import os
from unittest.mock import MagicMock

from cdisc_rules_engine.models.dictionaries import DictionaryTypes, AbstractTermsFactory
from cdisc_rules_engine.services.data_services.local_data_service import (
    LocalDataService,
)

dictionary_path = f"{os.path.dirname(__file__)}/../../../resources/dictionaries/medrt"


def test_install():
    storage_service = LocalDataService.get_instance(
        cache_service=MagicMock(), config=MagicMock()
    )
    factory = AbstractTermsFactory(storage_service).get_service(
        DictionaryTypes.MEDRT.value
    )
    dictionary = factory.install_terms(dictionary_path)
    assert dictionary.version == "2024.09.03"
    assert len(dictionary.items()) == 5088
