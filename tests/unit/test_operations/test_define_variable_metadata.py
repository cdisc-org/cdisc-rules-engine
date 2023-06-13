import pandas as pd
from cdisc_rules_engine.config.config import ConfigService
from pathlib import Path
from cdisc_rules_engine.operations.define_variable_metadata import (
    DefineVariableMetadata,
)
from cdisc_rules_engine.models.operation_params import OperationParams

from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)


def test_get_define_variable_metadata_variable_in_domain(
    operation_params: OperationParams,
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    resources_path: Path = Path(__file__).parent.parent.parent.joinpath("resources")
    operation_params.directory_path = str(resources_path)
    operation_params.domain = "AE"
    operation_params.target = "--SER"
    operation_params.attribute_name = "define_variable_ccode"
    result = DefineVariableMetadata(
        operation_params, pd.DataFrame.from_dict({"A": [1, 2, 3]}), cache, data_service
    ).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        val == "C49487"


def test_get_define_variable_metadata_variable_not_in_domain(
    operation_params: OperationParams,
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    resources_path: Path = Path(__file__).parent.parent.parent.joinpath("resources")
    operation_params.directory_path = str(resources_path)
    operation_params.domain = "AE"
    operation_params.target = "VERYFAKEVARIABLE"
    operation_params.attribute_name = "define_variable_ccode"
    result = DefineVariableMetadata(
        operation_params, pd.DataFrame.from_dict({"A": [1, 2, 3]}), cache, data_service
    ).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        val == ""
