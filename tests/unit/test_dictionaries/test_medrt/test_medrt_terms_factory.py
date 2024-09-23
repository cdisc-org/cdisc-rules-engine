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
    assert len(dictionary.items()) == 6
    names = [term.name for term in dictionary.values()]
    codes = [term.code for term in dictionary.values()]
    ids = [term.id for term in dictionary.values()]
    print(names)
    print(codes)
    print(ids)
    assert names == [
        "Cyclin-dependent Kinase 4 Inhibitors",
        "Cyclin-dependent Kinase 6 Inhibitors",
        "1-Compartment",
        "11 beta-Hydroxysteroid Dehydrogenase Inhibitors",
        "11 beta-Hydroxysteroid Dehydrogenase Type 1 Inhibitors",
        "11-beta Hydroxysteroid Dehydrogenase Inhibitors",
    ]
    assert codes == ["T58551", "T58554", "T82", "T58876", "T58869", "T58875"]
    assert ids == ["58551", "58554", "82", "58876", "58869", "58875"]
