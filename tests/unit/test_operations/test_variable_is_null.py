from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.operations.variable_is_null import VariableIsNull
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd
import pytest
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            pd.DataFrame.from_dict({"AEVAR": ["A", "B", "C"]}),
            False,
        ),
        (
            pd.DataFrame.from_dict({"AEVAR": [1, 2, 3]}),
            False,
        ),
        (
            pd.DataFrame.from_dict({"AEVAR": ["", None, "C"]}),
            False,
        ),
        (
            pd.DataFrame.from_dict({"AEVAR": [None, None, 3]}),
            False,
        ),
        (
            pd.DataFrame.from_dict({"AEVAR": ["", None]}),
            True,
        ),
        (
            pd.DataFrame.from_dict({"BCVAR": ["A", "B", "C"]}),
            True,
        ),
    ],
)
def test_variable_is_null(data, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.target = "--VAR"
    operation_params.domain = "AE"
    result = VariableIsNull(
        operation_params, pd.DataFrame.copy(data), cache, data_service
    ).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert val == expected
