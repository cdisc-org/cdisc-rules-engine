from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.exceptions.custom_exceptions import UnsupportedDictionaryType
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.dictionaries.base_external_dictionary import (
    ExternalDictionary,
)
from cdisc_rules_engine.models.dictionaries.medrt.term import MEDRTTerm
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.valid_external_dictionary_code_term_pair import (
    ValidExternalDictionaryCodeTermPair,
)
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.models.external_dictionaries_container import (
    ExternalDictionariesContainer,
    DictionaryTypes,
)
import pytest
from unittest import mock


@pytest.mark.parametrize("dataset_type", [(PandasDataset), (DaskDataset)])
def test_valid_external_dictionary_code_term_pairs(
    mock_data_service, operation_params: OperationParams, dataset_type
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    medrt_path = "medrt"
    operation_params.external_dictionary_type = "medrt"
    operation_params.dictionary_term_type = "PT"
    operation_params.original_target = "--COD"
    operation_params.target = "AECOD"
    operation_params.external_dictionary_term_variable = "AEDECOD"

    data = dataset_type.from_dict(
        {"AECOD": ["T1234", "T134", "T155"], "AEDECOD": ["A", "B", "B"]}
    )

    operation_params.dataframe = data
    operation_params.external_dictionaries = ExternalDictionariesContainer(
        {DictionaryTypes.MEDRT.value: medrt_path}
    )
    terms_dictionary = ExternalDictionary(
        {
            "T1234": MEDRTTerm(code="T1234", id=1, name="A"),
            "T134": MEDRTTerm(code="T134", id=2, name="B"),
            "T155": MEDRTTerm(code="T155", id=3, name="C"),
        }
    )
    cache.add(medrt_path, terms_dictionary)
    result = ValidExternalDictionaryCodeTermPair(
        operation_params,
        data,
        cache,
        mock_data_service,
    ).execute()
    assert result[operation_params.operation_id].tolist() == [True, True, False]


@pytest.mark.parametrize("dataset_type", [(PandasDataset), (DaskDataset)])
def test_valid_external_dictionary_value_with_invalid_external_dictionary_type(
    mock_data_service, operation_params: OperationParams, dataset_type
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    operation_params.external_dictionary_type = "invalid"
    operation_params.original_target = "--DECOD"
    operation_params.target = "AEDECOD"

    data = dataset_type.from_dict(
        {
            "AEDECOD": ["A", "B", "C"],
        }
    )

    operation_params.dataframe = data
    with pytest.raises(UnsupportedDictionaryType):
        ValidExternalDictionaryCodeTermPair(
            operation_params,
            data,
            cache,
            mock_data_service,
        ).execute()


@pytest.mark.parametrize("dataset_type", [(PandasDataset), (DaskDataset)])
def test_valid_external_dictionary_code_term_pairs_snomed(
    mock_data_service, operation_params: OperationParams, dataset_type
):
    mock_response = {
        "items": [
            {
                "conceptId": "T1234",
                "active": True,
                "definitionStatus": "PRIMITIVE",
                "moduleId": "900000000000207008",
                "effectiveTime": "20090731",
                "fsn": {"term": "BIO BRON MM (product)", "lang": "en"},
                "pt": {"term": "BIO BRON MM", "lang": "en"},
                "id": "T1234",
                "idAndFsnTerm": "99964002 | BIO BRON MM (product) |",
            },
            {
                "conceptId": "T134",
                "active": True,
                "definitionStatus": "PRIMITIVE",
                "moduleId": "900000000000207008",
                "effectiveTime": "20090731",
                "fsn": {"term": "BIOSOL AQUADROPS LIQUID (product)", "lang": "en"},
                "pt": {"term": "BIOSOL AQUADROPS LIQUID", "lang": "en"},
                "id": "T134",
                "idAndFsnTerm": "99987005 | BIOSOL AQUADROPS LIQUID (product) |",
            },
            {
                "conceptId": "T155",
                "active": True,
                "definitionStatus": "PRIMITIVE",
                "moduleId": "900000000000207008",
                "effectiveTime": "20090731",
                "fsn": {"term": "BIO SOTA + BRON MM (product)", "lang": "en"},
                "pt": {"term": "BIO SOTA + BRON MM", "lang": "en"},
                "id": "T155",
                "idAndFsnTerm": "99968004 | BIO SOTA + BRON MM (product) |",
            },
        ]
    }
    # mock_request_concepts.return_value = mock_response
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    operation_params.external_dictionary_type = DictionaryTypes.SNOMED.value
    operation_params.original_target = "--VALCD"
    operation_params.target = "TSVALCD"
    operation_params.external_dictionary_term_variable = "TSVAL"

    data = dataset_type.from_dict(
        {
            "TSVALCD": ["T1234", "T134", "T155"],
            "TSVAL": ["BIO BRON MM", "BIOSOL AQUADROPS LIQUID (product)", "invalid"],
        }
    )

    operation_params.dataframe = data
    operation_params.external_dictionaries = ExternalDictionariesContainer(
        {DictionaryTypes.SNOMED.value: {"edition": "test", "version": "test"}}
    )
    with mock.patch(
        "cdisc_rules_engine.models.dictionaries.snomed.terms_factory.SNOMEDTermsFactory._request_concepts",
        return_value=mock_response,
    ):
        result = ValidExternalDictionaryCodeTermPair(
            operation_params,
            data,
            cache,
            mock_data_service,
        ).execute()
        assert result[operation_params.operation_id].tolist() == [True, True, False]
