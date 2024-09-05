from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.operations.day_data_validator import DayDataValidator
from cdisc_rules_engine.models.operation_params import OperationParams
import pytest
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset

from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory


@pytest.mark.parametrize(
    "data, dataset_type, expected",
    [
        (
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
                "DOMAIN": ["DM", "DM", "DM", "DM", "DM", "DM", "DM"],
            },
            PandasDataset,
            [4, 32, 1, 13, "", "", -1],
        ),
        (
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
                "DOMAIN": ["DM", "DM", "DM", "DM", "DM", "DM", "DM"],
            },
            DaskDataset,
            [4, 32, 1, 13, "", "", -1],
        ),
    ],
)
def test_day_data_calculation(
    data, dataset_type, expected, mock_data_service, operation_params: OperationParams
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    datasets_map = {
        "dm.xpt": dataset_type.from_dict(
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
        {"domain": "DM", "filename": "dm.xpt", "full_path": "/path/to/dm.xpt"},
    ]
    mock_data_service.get_dataset.side_effect = (
        lambda *args, **kwargs: datasets_map.get(
            args.split("/")[-1]
            if args
            else kwargs.get("dataset_name", "").split("/")[-1]
        )
    )
    operation_params.datasets = datasets
    operation_params.dataframe = PandasDataset.from_dict(data)
    operation_params.target = "values"
    result = DayDataValidator(
        operation_params, PandasDataset.from_dict(data), cache, mock_data_service
    ).execute()
    assert operation_params.operation_id in result
    for i, val in enumerate(result[operation_params.operation_id]):
        assert val == expected[i]
