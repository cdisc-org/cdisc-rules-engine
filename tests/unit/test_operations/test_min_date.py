from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.min_date import MinDate
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.check_operators.helpers import format_date_preserving_precision
import pytest

from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)


@pytest.mark.parametrize(
    "data, expected, dataset_type, grouping",
    [
        (
            {"dates": ["2001-01-01", "", "2022-01-01"]},
            format_date_preserving_precision("2001-01-01"),
            DaskDataset,
            None,
        ),
        ({"dates": [None, None]}, "", DaskDataset, None),
        (
            {"dates": ["2001-01-01", "", "2022-01-01"]},
            format_date_preserving_precision("2001-01-01"),
            PandasDataset,
            None,
        ),
        ({"dates": [None, None]}, "", PandasDataset, None),
        (
            {
                "dates": ["2025-10-10", "2025-10-15", "2025-12-02", "2025-12-11"],
                "USUBJID": ["00002", "00002", "00003", "00003"],
            },
            PandasDataset.from_records(
                [
                    {
                        "dates": "2025-10-10",
                        "USUBJID": "00002",
                        "operation_id": "2025-10-10",
                    },
                    {
                        "dates": "2025-10-15",
                        "USUBJID": "00002",
                        "operation_id": "2025-10-10",
                    },
                    {
                        "dates": "2025-12-02",
                        "USUBJID": "00003",
                        "operation_id": "2025-12-02",
                    },
                    {
                        "dates": "2025-12-11",
                        "USUBJID": "00003",
                        "operation_id": "2025-12-02",
                    },
                ]
            ),
            PandasDataset,
            ["USUBJID"],
        ),
        (
            {
                "dates": ["2025-10-10", "2025-10-15", "2025-12-02", "2025-12-11"],
                "USUBJID": ["00002", "00002", "00003", "00003"],
            },
            DaskDataset.from_records(
                [
                    {
                        "dates": "2025-10-10",
                        "USUBJID": "00002",
                        "operation_id": "2025-10-10",
                    },
                    {
                        "dates": "2025-10-15",
                        "USUBJID": "00002",
                        "operation_id": "2025-10-10",
                    },
                    {
                        "dates": "2025-12-02",
                        "USUBJID": "00003",
                        "operation_id": "2025-12-02",
                    },
                    {
                        "dates": "2025-12-11",
                        "USUBJID": "00003",
                        "operation_id": "2025-12-02",
                    },
                ]
            ),
            DaskDataset,
            ["USUBJID"],
        ),
        (
            {
                "dates": [
                    "2025-10-10",
                    "2025-10-15",
                    "2025-12-02",
                    "2025-12-11",
                    "",
                    "",
                ],
                "USUBJID": ["00002", "00002", "00003", "00003", "00004", "00004"],
            },
            PandasDataset.from_records(
                [
                    {
                        "dates": "2025-10-10",
                        "USUBJID": "00002",
                        "operation_id": "2025-10-10",
                    },
                    {
                        "dates": "2025-10-15",
                        "USUBJID": "00002",
                        "operation_id": "2025-10-10",
                    },
                    {
                        "dates": "2025-12-02",
                        "USUBJID": "00003",
                        "operation_id": "2025-12-02",
                    },
                    {
                        "dates": "2025-12-11",
                        "USUBJID": "00003",
                        "operation_id": "2025-12-02",
                    },
                    {"dates": "", "USUBJID": "00004", "operation_id": ""},
                    {"dates": "", "USUBJID": "00004", "operation_id": ""},
                ]
            ),
            PandasDataset,
            ["USUBJID"],
        ),
        (
            {
                "dates": [
                    "2025-10-10",
                    "2025-10-15",
                    "2025-12-02",
                    "2025-12-11",
                    "",
                    "",
                ],
                "USUBJID": ["00002", "00002", "00003", "00003", "00004", "00004"],
            },
            DaskDataset.from_records(
                [
                    {
                        "dates": "2025-10-10",
                        "USUBJID": "00002",
                        "operation_id": "2025-10-10",
                    },
                    {
                        "dates": "2025-10-15",
                        "USUBJID": "00002",
                        "operation_id": "2025-10-10",
                    },
                    {
                        "dates": "2025-12-02",
                        "USUBJID": "00003",
                        "operation_id": "2025-12-02",
                    },
                    {
                        "dates": "2025-12-11",
                        "USUBJID": "00003",
                        "operation_id": "2025-12-02",
                    },
                    {"dates": "", "USUBJID": "00004", "operation_id": ""},
                    {"dates": "", "USUBJID": "00004", "operation_id": ""},
                ]
            ),
            DaskDataset,
            ["USUBJID"],
        ),
    ],
)
def test_minimum(
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
    result = MinDate(
        operation_params, dataset_type.from_dict(data), cache, data_service
    ).execute()
    assert operation_params.operation_id in result

    if isinstance(expected, PandasDataset) and dataset_type is PandasDataset:
        assert result.data.equals(expected.data)
    elif isinstance(expected, DaskDataset) and dataset_type is DaskDataset:
        assert expected.equals(result)
    else:
        for val in result[operation_params.operation_id]:
            assert val == expected
