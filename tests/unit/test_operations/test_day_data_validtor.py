from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd
import dask.dataframe as dd
import pytest

from cdisc_rules_engine.DatasetOperations.Operations import DatasetOperations
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            pd.DataFrame.from_dict(
                {
                    "values": [
                        "1997-07-19T19:20:30",
                        "1997-08-16T19:20:30",
                        "1997-07-16T19:20",
                        "2022-05-20T13:44",
                        "2022-05-20T13:44",
                        None,
                        "2022-05-19T13:44",
                    ],
                    "USUBJID": [1, 2, 3, 4, 5, 6, 7],
                }
            ),
            [4, 32, 1, 13, "", "", -1],
        ),
        (
            dd.DataFrame.from_dict(
                {
                    "values": [
                        "1997-07-19T19:20:30",
                        "1997-08-16T19:20:30",
                        "1997-07-16T19:20",
                        "2022-05-20T13:44",
                        "2022-05-20T13:44",
                        None,
                        "2022-05-19T13:44",
                    ],
                    "USUBJID": [1, 2, 3, 4, 5, 6, 7],
                },
                npartitions=1,
            ),
            [4, 32, 1, 13, "", "", -1],
        ),
    ],
)
def test_day_data_calculation(
    data, expected, mock_data_service, operation_params: OperationParams
):
    operations = DatasetOperations()
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    datasets_map = {
        "dm.xpt": pd.DataFrame.from_dict(
            {
                "RFSTDTC": [
                    "1997-07-16T19:20:30",
                    "1997-07-16T19:20:30",
                    "1997-07-16T19:20",
                    "2022-05-08T13:44",
                    "TEST",
                    "2022-05-20T13:44",
                    "2022-05-20T13:44",
                ],
                "USUBJID": [1, 2, 3, 4, 5, 6, 7],
            }
        )
    }
    datasets = [
        {"domain": "DM", "filename": "dm.xpt"},
    ]
    mock_data_service.get_dataset.side_effect = lambda name: datasets_map.get(
        name.split("/")[-1]
    )
    operation_params.datasets = datasets
    operation_params.dataframe = data
    operation_params.target = "values"
    result = operations.get_service(
        "dy", operation_params, data, cache, mock_data_service
    )
    assert operation_params.operation_id in result
    for i, val in enumerate(result[operation_params.operation_id]):
        assert val == expected[i]
