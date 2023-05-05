import pytest
import pandas as pd

from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.domain_is_custom import DomainIsCustom
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.utilities.utils import (
    get_standard_details_cache_key,
)


@pytest.mark.parametrize(
    "dataframe, domain, standard, standard_version, expected",
    [
        (
            pd.DataFrame.from_dict(
                {
                    "STUDYID": [
                        "TEST_STUDY",
                        "TEST_STUDY",
                        "TEST_STUDY",
                    ],
                    "AETERM": [
                        "test",
                        "test",
                        "test",
                    ],
                }
            ),
            "AE",
            "sdtmig",
            "3-4",
            False,
        ),
        (
            pd.DataFrame.from_dict(
                {
                    "STUDYID": [
                        "TEST_STUDY",
                        "TEST_STUDY",
                        "TEST_STUDY",
                    ],
                    "BCTERM": [
                        "test",
                        "test",
                        "test",
                    ],
                }
            ),
            "BC",
            "sdtmig",
            "3-4",
            True,
        ),
    ],
)
def test_domain_is_custom(
    operation_params: OperationParams,
    dataframe: pd.DataFrame,
    domain: str,
    standard: str,
    standard_version: str,
    expected: bool,
):
    standard_metadata = {
        "domains": {"AE"},
    }
    operation_params.dataframe = dataframe
    operation_params.domain = domain
    operation_params.standard = standard
    operation_params.standard_version = standard_version
    # save model metadata to cache
    cache = InMemoryCacheService.get_instance()
    cache.add(
        get_standard_details_cache_key(
            operation_params.standard, operation_params.standard_version
        ),
        standard_metadata,
    )
    # execute operation
    data_service = LocalDataService.get_instance(cache_service=cache)
    operation = DomainIsCustom(
        operation_params, operation_params.dataframe, cache, data_service
    )
    result: pd.DataFrame = operation.execute()
    assert result[operation_params.operation_id].equals(
        pd.Series([expected, expected, expected])
    )
