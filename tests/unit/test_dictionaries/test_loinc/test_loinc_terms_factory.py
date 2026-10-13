import io
import os
from unittest.mock import MagicMock

import pytest

from cdisc_rules_engine.exceptions.custom_exceptions import MissingDataError
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


def test_install_raises_on_row_with_missing_fields():
    """A Loinc.csv data row with fewer than 9 fields must raise MissingDataError,
    not return the exception object (which callers store as the dictionary)."""
    data_service = MagicMock()
    data_service.has_all_files.return_value = True
    data_service.read_data.return_value = io.BytesIO(
        b"NUM,COMP,PROP,TIME,SYS,SCALE,METHOD,CLASS,VER\n100000-9,short\n"
    )
    factory = AbstractTermsFactory(data_service).get_service(
        DictionaryTypes.LOINC.value
    )
    with pytest.raises(MissingDataError):
        factory.install_terms("some/path")
