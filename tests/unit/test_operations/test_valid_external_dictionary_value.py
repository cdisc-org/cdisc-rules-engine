from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.exceptions.custom_exceptions import UnsupportedDictionaryType
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd
import dask.dataframe as dd
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.models.dictionaries.meddra.terms.term_types import TermTypes
from cdisc_rules_engine.models.dictionaries.meddra.terms.meddra_term import MedDRATerm
from cdisc_rules_engine.DatasetOperations.Operations import DatasetOperations
import pytest


def test_valid_external_dictionary_value_with_meddra(
    mock_data_service, operation_params: OperationParams
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    meddra_path = "meddra"
    operation_params.external_dictionary_type = "meddra"
    operation_params.dictionary_term_type = "PT"
    operation_params.original_target = "--DECOD"
    operation_params.target = "AEDECOD"

    data = pd.DataFrame.from_dict(
        {
            "AEDECOD": ["A", "B", "C"],
        }
    )

    operation_params.dataframe = data
    operation_params.meddra_path = meddra_path
    terms_dictionary = {
        TermTypes.PT.value: {
            "1234": MedDRATerm({"term": "A"}),
            "134": MedDRATerm({"term": "B"}),
        },
    }
    cache.add(meddra_path, terms_dictionary)
    operations = DatasetOperations()
    result = operations.get_service(
        "valid_external_dictionary_value",
        operation_params,
        data,
        cache,
        mock_data_service,
    )
    assert result[operation_params.operation_id].tolist() == [True, True, False]


def test_valid_external_dictionary_value_with_invalid_external_dictionary_type(
    mock_data_service, operation_params: OperationParams
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    operation_params.external_dictionary_type = "invalid"
    operation_params.original_target = "--DECOD"
    operation_params.target = "AEDECOD"

    data = pd.DataFrame.from_dict(
        {
            "AEDECOD": ["A", "B", "C"],
        }
    )

    operation_params.dataframe = data
    operations = DatasetOperations()
    with pytest.raises(UnsupportedDictionaryType):
        operations.get_service(
            "valid_external_dictionary_value",
            operation_params,
            data,
            cache,
            mock_data_service,
        )


def test_valid_external_dictionary_value_with_meddra_dask(
    mock_data_service, operation_params: OperationParams
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    meddra_path = "meddra"
    operation_params.external_dictionary_type = "meddra"
    operation_params.dictionary_term_type = "PT"
    operation_params.original_target = "--DECOD"
    operation_params.target = "AEDECOD"

    data = dd.DataFrame.from_dict(
        {
            "AEDECOD": ["A", "B", "C"],
        },
        npartitions=1,
    )

    operation_params.dataframe = data
    operation_params.meddra_path = meddra_path
    terms_dictionary = {
        TermTypes.PT.value: {
            "1234": MedDRATerm({"term": "A"}),
            "134": MedDRATerm({"term": "B"}),
        },
    }
    cache.add(meddra_path, terms_dictionary)
    operations = DatasetOperations()
    result = operations.get_service(
        "valid_external_dictionary_value",
        operation_params,
        data,
        cache,
        mock_data_service,
    )
    for res, exp in zip(result[operation_params.operation_id], [True, True, False]):
        assert res == exp
