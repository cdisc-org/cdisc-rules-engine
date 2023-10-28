import pandas as pd
import dask.dataframe as dd
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.DatasetOperations.Operations import DatasetOperations
from unittest.mock import Mock
import pytest


@pytest.mark.parametrize(
    "dataframe",
    [
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
        dd.DataFrame.from_dict(
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
        ),
    ],
)
def test_extract_metadata_get_dataset_name(
    operation_params: OperationParams, dataframe
):
    mock_data_service = Mock(LocalDataService)
    mock_data_service.get_dataset_metadata.return_value = pd.DataFrame.from_dict(
        {"dataset_name": ["AE"]}
    )
    operation_params.dataframe = dataframe
    operation_params.target = "dataset_name"
    cache = InMemoryCacheService.get_instance()
    operation = DatasetOperations()
    # execute operation
    result = operation.get_service(
        "extract_metadata",
        operation_params,
        operation_params.dataframe,
        cache,
        mock_data_service,
    )
    assert operation_params.operation_id in result
    for item in result[operation_params.operation_id]:
        assert item == "AE"
