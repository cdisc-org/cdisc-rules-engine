from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd
import dask.dataframe as dd

from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)
from cdisc_rules_engine.DatasetOperations.Operations import DatasetOperations


# def test_valid_whodrug_references_pandas(
#     installed_whodrug_dictionaries: dict, operation_params: OperationParams
# ):
#     """
#     Unit test for valid_whodrug_references function.
#     """
#     config = ConfigService()
#     cache = CacheServiceFactory(config).get_cache_service()
#     data_service = DataServiceFactory(config, cache).get_data_service()
#     # create a dataset where 2 rows reference invalid terms
#     invalid_df = pd.DataFrame.from_dict(
#         {
#             "DOMAIN": [
#                 "AE",
#                 "AE",
#                 "AE",
#                 "AE",
#             ],
#             "AEINA": ["A", "A01", "A01AC", "A01AD"],
#         }
#     )

#     operation_params.dataframe = invalid_df
#     operation_params.target = "AEINA"
#     operation_params.domain = "AE"
#     operation_params.whodrug_path = installed_whodrug_dictionaries["whodrug_path"]
#     operations = DatasetOperations()
#     result = operations.get_service(
#         "valid_whodrug_references", operation_params, invalid_df, cache, data_service
#     )
#     assert operation_params.operation_id in result
#     assert result[operation_params.operation_id].equals(
#         pd.Series([True, True, False, False])
#     )


def test_valid_whodrug_references_dask(
    installed_whodrug_dictionaries: dict, operation_params: OperationParams
):
    """
    Unit test for valid_whodrug_references function.
    """
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    # create a dataset where 2 rows reference invalid terms
    invalid_df = dd.DataFrame.from_dict(
        {
            "DOMAIN": [
                "AE",
                "AE",
                "AE",
                "AE",
            ],
            "AEINA": ["A", "A01", "A01AC", "A01AD"],
        },
        npartitions=1,
    )

    operation_params.dataframe = invalid_df
    operation_params.target = "AEINA"
    operation_params.domain = "AE"
    operation_params.whodrug_path = installed_whodrug_dictionaries["whodrug_path"]
    operations = DatasetOperations()
    result = operations.get_service(
        "valid_whodrug_references", operation_params, invalid_df, cache, data_service
    )
    assert operation_params.operation_id in result
    expected = pd.Series([True, True, False, False])
    print(result[operation_params.operation_id])

    for res, exp in zip(result[operation_params.operation_id], expected):
        assert res == exp
