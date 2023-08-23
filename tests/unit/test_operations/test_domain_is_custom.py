from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
import pytest
import pandas as pd

from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.domain_is_custom import DomainIsCustom
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService


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
    library_metadata = LibraryMetadataContainer(standard_metadata=standard_metadata)
    # execute operation
    data_service = LocalDataService.get_instance(
        cache_service=cache, config=ConfigService()
    )
    operation = DomainIsCustom(
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )
    result: pd.DataFrame = operation.execute()
    assert result[operation_params.operation_id].equals(
        pd.Series([expected, expected, expected])
    )
