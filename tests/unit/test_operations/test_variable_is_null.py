from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.variable_is_null import VariableIsNull
from cdisc_rules_engine.models.operation_params import OperationParams
import pytest
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            PandasDataset.from_dict({"AEVAR": ["A", "B", "C"]}),
            False,
        ),
        (
            DaskDataset.from_dict({"AEVAR": [1, 2, 3]}),
            False,
        ),
        (
            DaskDataset.from_dict({"AEVAR": ["", None, "C"]}),
            False,
        ),
        (
            PandasDataset.from_dict({"AEVAR": [None, None, 3]}),
            False,
        ),
        (
            PandasDataset.from_dict({"AEVAR": ["", None]}),
            True,
        ),
        (
            DaskDataset.from_dict({"BCVAR": ["A", "B", "C"]}),
            True,
        ),
    ],
)
def test_variable_is_null(
    data, expected, mock_data_service, operation_params: OperationParams
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    operation_params.dataframe = data
    operation_params.target = "--VAR"
    operation_params.domain = "AE"
    mock_data_service.get_dataset.return_value = data
    mock_data_service.dataset_implementation = data.__class__
    result = VariableIsNull(operation_params, data, cache, mock_data_service).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert val == expected


def test_define_crosscheck_variable_is_null(mock_data_service, operation_params):
    define_metadata = PandasDataset.from_dict(
        {
            "define_variable_name": ["AEHLT", "AETERM"],
            "define_variable_has_no_data": ["Yes", "No"],
        }
    )
    dataset = PandasDataset.from_dict({"AEHLT": [None, None], "AETERM": [1, 2]})
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    operation_params.dataframe = define_metadata
    operation_params.target = "define_variable_name"
    mock_data_service.get_dataset.return_value = dataset
    mock_data_service.dataset_implementation = PandasDataset
    result = VariableIsNull(
        operation_params, PandasDataset(define_metadata.data), cache, mock_data_service
    ).execute()
    assert operation_params.operation_id in result
    assert result[operation_params.operation_id].to_list() == [True, False]
