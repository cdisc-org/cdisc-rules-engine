from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
import pandas as pd
import dask.dataframe as dd
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.DatasetOperations.Operations import DatasetOperations


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
    operations = DatasetOperations()
    result = operations.get_service(
        "domain_label",
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )
    expected: pd.Series = pd.Series(
        [
            "Adverse Events",
            "Adverse Events",
            "Adverse Events",
        ]
    )
    assert result[operation_params.operation_id].equals(expected)
    # test for dask
    operation_params.dataframe = dd.DataFrame.from_dict(
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
        },
        npartitions=1,
    )
    result = operations.get_service(
        "domain_label",
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )
    for column in result.columns:
        result[column] = result[column].astype(object)
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
    operations = DatasetOperations()
    result = operations.get_service(
        "domain_label",
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )
    expected: pd.Series = pd.Series(
        [
            "",
            "",
            "",
        ]
    )
    assert result[operation_params.operation_id].equals(expected)

    # test dask
    operation_params.dataframe = dd.DataFrame.from_dict(
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
        },
        npartitions=1,
    )
    result = operations.get_service(
        "domain_label",
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )
    for column in result.columns:
        result[column] = result[column].astype(object)
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
        },
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
    operations = DatasetOperations()
    result = operations.get_service(
        "domain_label",
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )
    expected: pd.Series = pd.Series(
        [
            "",
            "",
            "",
        ]
    )
    assert result[operation_params.operation_id].equals(expected)

    # test for dask
    operation_params.dataframe = dd.DataFrame.from_dict(
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
        },
        npartitions=1,
    )
    result = operations.get_service(
        "domain_label",
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )
    for column in result.columns:
        result[column] = result[column].astype(object)
    assert result[operation_params.operation_id].equals(expected)
