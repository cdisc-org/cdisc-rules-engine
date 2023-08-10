from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.exceptions.custom_exceptions import UnsupportedDictionaryType
from cdisc_rules_engine.operations.valid_external_dictionary_value import (
    ValidExternalDictionaryValue,
)
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.models.dictionaries.meddra.terms.term_types import TermTypes
from cdisc_rules_engine.models.dictionaries.meddra.terms.meddra_term import MedDRATerm
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
    result = ValidExternalDictionaryValue(
        operation_params,
        data,
        cache,
        mock_data_service,
    ).execute()
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
    with pytest.raises(UnsupportedDictionaryType):
        ValidExternalDictionaryValue(
            operation_params,
            data,
            cache,
            mock_data_service,
        ).execute()
