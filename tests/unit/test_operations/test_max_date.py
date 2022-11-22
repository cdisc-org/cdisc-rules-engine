from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.operations.max_date import MaxDate
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
            pd.DataFrame.from_dict({"dates": ["2001-01-01", "", "2022-01-05"]}),
            pd.to_datetime("2022-01-05").isoformat(),
        ),
        (pd.DataFrame.from_dict({"dates": [None, None]}), ""),
    ],
)
def test_minimum(data, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.target = "dates"
    result = MaxDate(
        operation_params, pd.DataFrame.copy(data), cache, data_service
    ).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert val == expected
