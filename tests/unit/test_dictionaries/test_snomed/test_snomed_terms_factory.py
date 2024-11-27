from unittest.mock import MagicMock, patch

from cdisc_rules_engine.models.dictionaries import DictionaryTypes, AbstractTermsFactory
from cdisc_rules_engine.services.data_services.local_data_service import (
    LocalDataService,
)


@patch(
    "cdisc_rules_engine.models.dictionaries.snomed.terms_factory.SNOMEDTermsFactory._request_concepts"
)
def test_install(mock_request_concepts):
    mock_response = {
        "items": [
            {
                "conceptId": "99964002",
                "active": True,
                "definitionStatus": "PRIMITIVE",
                "moduleId": "900000000000207008",
                "effectiveTime": "20090731",
                "fsn": {"term": "BIO BRON MM (product)", "lang": "en"},
                "pt": {"term": "BIO BRON MM", "lang": "en"},
                "id": "99964002",
                "idAndFsnTerm": "99964002 | BIO BRON MM (product) |",
            },
            {
                "conceptId": "99987005",
                "active": True,
                "definitionStatus": "PRIMITIVE",
                "moduleId": "900000000000207008",
                "effectiveTime": "20090731",
                "fsn": {"term": "BIOSOL AQUADROPS LIQUID (product)", "lang": "en"},
                "pt": {"term": "BIOSOL AQUADROPS LIQUID", "lang": "en"},
                "id": "99987005",
                "idAndFsnTerm": "99987005 | BIOSOL AQUADROPS LIQUID (product) |",
            },
            {
                "conceptId": "99968004",
                "active": True,
                "definitionStatus": "PRIMITIVE",
                "moduleId": "900000000000207008",
                "effectiveTime": "20090731",
                "fsn": {"term": "BIO SOTA + BRON MM (product)", "lang": "en"},
                "pt": {"term": "BIO SOTA + BRON MM", "lang": "en"},
                "id": "99968004",
                "idAndFsnTerm": "99968004 | BIO SOTA + BRON MM (product) |",
            },
        ]
    }
    mock_request_concepts.return_value = mock_response
    storage_service = LocalDataService.get_instance(
        cache_service=MagicMock(), config=MagicMock()
    )
    factory = AbstractTermsFactory(storage_service).get_service(
        DictionaryTypes.SNOMED.value, edition="SNOMEDCT-US", version="2024-09-01"
    )
    dictionary = factory.install_terms("test", concepts=["1", "2"])
    assert dictionary.version == "MAIN/SNOMEDCT-US/2024-09-01"
    assert len(dictionary.items()) == 3
    codes = [term.concept_id for term in dictionary.values()]
    assert codes == ["99964002", "99987005", "99968004"]
