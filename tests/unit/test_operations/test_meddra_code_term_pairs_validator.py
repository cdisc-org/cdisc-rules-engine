from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd

from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)
from cdisc_rules_engine.DatasetOperations.Operations import DatasetOperations


def test_meddra_code_term_pairs_validator(
    installed_meddra_dictionaries: dict, operation_params: OperationParams
):
    """
    Unit test for valid_whodrug_references function.
    """
    config = ConfigService()
    cache = installed_meddra_dictionaries["cache_service"]
    data_service = DataServiceFactory(config, cache).get_data_service()
    # create a dataset where 2 rows reference invalid terms
    invalid_df = pd.DataFrame.from_dict(
        {"AELLTCD": ["TEST1", "TEST2", "LLT3"], "AELLT": ["TEST1", "TEST2", "TESTLLT3"]}
    )

    operation_params.dataframe = invalid_df
    operation_params.domain = "AE"
    operation_params.target = "AELLT"
    operation_params.meddra_path = installed_meddra_dictionaries["meddra_path"]
    operations = DatasetOperations()
    result = operations.get_service(
        "valid_meddra_code_term_pairs",
        operation_params,
        invalid_df,
        cache,
        data_service,
    )
    assert operation_params.operation_id in result
    assert result[operation_params.operation_id].equals(pd.Series([False, False, True]))
