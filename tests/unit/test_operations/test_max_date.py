from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.max_date import MaxDate
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd
import pytest

from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)


@pytest.mark.parametrize(
    "data, expected, dataset_type, grouping",
    [
        (
            {"dates": ["2001-01-01", "", "2022-01-05"]},
            pd.to_datetime("2022-01-05").isoformat(),
            PandasDataset,
            None,
        ),
        ({"dates": [None, None]}, "", PandasDataset, None),
        (
            {"dates": ["2001-01-01", "", "2022-01-05"]},
            pd.to_datetime("2022-01-05").isoformat(),
            DaskDataset,
            None,
        ),
        ({"dates": [None, None]}, "", DaskDataset, None),
        (
            {
                "dates": ["2025-10-10", "2025-10-15", "2025-12-02", "2025-12-11"],
                "USUBJID": ["00002", "00002", "00003", "00003"],
            },
            pd.Series(["2025-10-15", "2025-10-15", "2025-12-11", "2025-12-11"]),
            PandasDataset,
            "USUBJID",
        ),
    ],
)
def test_max_date(
    data,
    expected,
    dataset_type,
    grouping: str | None,
    operation_params: OperationParams,
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = dataset_type.from_dict(data)
    operation_params.target = "dates"
    operation_params.grouping = grouping
    result = MaxDate(
        operation_params, dataset_type.from_dict(data), cache, data_service
    ).execute()
    assert operation_params.operation_id in result

    if isinstance(expected, pd.Series):
        assert result[operation_params.operation_id].equals(expected)
    else:
        for val in result[operation_params.operation_id]:
            assert val == expected
