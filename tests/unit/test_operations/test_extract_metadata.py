import pandas as pd
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.extract_metadata import ExtractMetadata
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService
from unittest.mock import Mock
import pytest


@pytest.mark.parametrize("dataset_type", [(PandasDataset), (DaskDataset)])
def test_extract_metadata_get_dataset_name(
    operation_params: OperationParams, dataset_type
):
    mock_data_service = Mock(LocalDataService)
    mock_data_service.get_dataset_metadata.return_value = pd.DataFrame.from_dict(
        {"dataset_name": ["AE"]}
    )
    operation_params.dataframe = dataset_type.from_dict(
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
    operation_params.target = "dataset_name"
    cache = InMemoryCacheService.get_instance()
    # execute operation
    operation = ExtractMetadata(
        operation_params, operation_params.dataframe, cache, mock_data_service
    )
    result: pd.DataFrame = operation.execute()
    assert operation_params.operation_id in result
    for item in result[operation_params.operation_id]:
        assert item == "AE"
