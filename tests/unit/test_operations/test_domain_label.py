from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
import pandas as pd
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.domain_label import DomainLabel
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService


def test_get_domain_label_from_library(operation_params: OperationParams):
    standard_metadata = {
        "_links": {"model": {"href": "/mdr/sdtm/1-5"}},
        "classes": [
            {
                "name": "Events",
                "datasets": [
                    {
                        "name": "AE",
                        "label": "Adverse Events",
                        "datasetVariables": [
                            {"name": "AETEST", "ordinal": 1},
                            {"name": "AENEW", "ordinal": 2},
                        ],
                    }
                ],
            }
        ],
    }
    operation_params.dataframe = pd.DataFrame.from_dict(
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
    )
    operation_params.domain = "AE"
    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"

    # save model metadata to cache
    cache = InMemoryCacheService.get_instance()
    library_metadata = LibraryMetadataContainer(standard_metadata=standard_metadata)
    # execute operation
    data_service = LocalDataService.get_instance(
        cache_service=cache, config=ConfigService()
    )
    operation = DomainLabel(
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )
    result: pd.DataFrame = operation.execute()
    expected: pd.Series = pd.Series(
        [
            "Adverse Events",
            "Adverse Events",
            "Adverse Events",
        ]
    )
    assert result[operation_params.operation_id].equals(expected)


def test_get_domain_label_from_library_domain_not_found(
    operation_params: OperationParams,
):
    standard_metadata = {
        "_links": {"model": {"href": "/mdr/sdtm/1-5"}},
        "classes": [
            {
                "name": "Events",
                "datasets": [
                    {
                        "name": "AE",
                        "label": "Adverse Events",
                        "datasetVariables": [
                            {"name": "AETEST", "ordinal": 1},
                            {"name": "AENEW", "ordinal": 2},
                        ],
                    }
                ],
            }
        ],
    }
    operation_params.dataframe = pd.DataFrame.from_dict(
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
    )
    operation_params.domain = "VS"
    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"

    # save model metadata to cache
    cache = InMemoryCacheService.get_instance()
    # execute operation
    library_metadata = LibraryMetadataContainer(standard_metadata=standard_metadata)
    # execute operation
    data_service = LocalDataService.get_instance(
        cache_service=cache, config=ConfigService()
    )
    operation = DomainLabel(
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )
    result: pd.DataFrame = operation.execute()
    expected: pd.Series = pd.Series(
        [
            "",
            "",
            "",
        ]
    )
    assert result[operation_params.operation_id].equals(expected)


def test_get_domain_label_from_library_domain_missing_label(
    operation_params: OperationParams,
):
    standard_metadata = {
        "_links": {"model": {"href": "/mdr/sdtm/1-5"}},
        "classes": [
            {
                "name": "Events",
                "datasets": [
                    {
                        "name": "AE",
                        "datasetVariables": [
                            {"name": "AETEST", "ordinal": 1},
                            {"name": "AENEW", "ordinal": 2},
                        ],
                    }
                ],
            }
        ],
    }
    operation_params.dataframe = pd.DataFrame.from_dict(
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
    )
    operation_params.domain = "AE"
    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"

    # save model metadata to cache
    cache = InMemoryCacheService.get_instance()
    # execute operation
    library_metadata = LibraryMetadataContainer(standard_metadata=standard_metadata)
    # execute operation
    data_service = LocalDataService.get_instance(
        cache_service=cache, config=ConfigService()
    )
    operation = DomainLabel(
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )
    result: pd.DataFrame = operation.execute()
    expected: pd.Series = pd.Series(
        [
            "",
            "",
            "",
        ]
    )
    assert result[operation_params.operation_id].equals(expected)
