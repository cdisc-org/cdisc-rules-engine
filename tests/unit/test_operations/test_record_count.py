from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.operations.record_count import RecordCount
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
            pd.DataFrame.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01"],
                    "DOMAIN": ["AE", "AE"],
                    "EQ": [1, 2],
                    "USUBJID": ["TEST1", "TEST1"],
                }
            ),
            pd.DataFrame.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01"],
                    "DOMAIN": ["AE", "AE"],
                    "EQ": [1, 2],
                    "USUBJID": ["TEST1", "TEST1"],
                    "operation_id": [2, 2],
                }
            ),
        ),
    ],
)
def test_record_count_operation(data, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    result = RecordCount(operation_params, data, cache, data_service).execute()
    assert operation_params.operation_id in result
    assert result.equals(expected)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            pd.DataFrame.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE"],
                    "EQ": [1, 2],
                    "USUBJID": ["TEST1", "TEST1"],
                }
            ),
            pd.DataFrame.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE"],
                    "EQ": [1, 2],
                    "USUBJID": ["TEST1", "TEST1"],
                    "operation_id": [1, 1],
                }
            ),
        ),
    ],
)
def test_filtered_record_count(data, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.filter = {"STUDYID": "CDISC02"}
    result = RecordCount(operation_params, data, cache, data_service).execute()
    assert operation_params.operation_id in result
    assert result.equals(expected)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            pd.DataFrame.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 2],
                    "USUBJID": ["TEST1", "TEST1", "TEST2"],
                }
            ),
            pd.DataFrame.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 2],
                    "USUBJID": ["TEST1", "TEST1", "TEST2"],
                    "operation_id": [2, 2, 1],
                }
            ),
        ),
    ],
)
def test_grouped_record_count(data, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.grouping = ["STUDYID"]
    result = RecordCount(operation_params, data, cache, data_service).execute()
    assert operation_params.operation_id in result
    assert result.equals(expected)
