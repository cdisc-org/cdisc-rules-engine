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
import pytest


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
    operation_params.medrt_path = medrt_path
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
