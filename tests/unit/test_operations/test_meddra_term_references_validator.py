from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.meddra_term_references_validator import (
    MedDRATermReferencesValidator,
)
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd

from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)
import pytest


@pytest.mark.parametrize("dataset_type", [(PandasDataset), (DaskDataset)])
def test_meddra_term_references_validator(
    installed_meddra_dictionaries: dict, operation_params: OperationParams, dataset_type
):
    """
    Unit test for valid_whodrug_references function.
    """
    config = ConfigService()
    cache = installed_meddra_dictionaries["cache_service"]
    data_service = DataServiceFactory(config, cache).get_data_service()
    # create a dataset where 2 rows reference invalid terms
    invalid_df = dataset_type.from_dict(
        {
            "AELLT": ["TEST1", "TEST2", "TESTLLT3"],
            "AESOC": ["TEST1", "TEST2", "TESTSOC3"],
            "AEHLGT": ["TEST1", "TEST2", "TESTHLGT3"],
            "AEHLT": ["TEST1", "TEST2", "TESTHLT3"],
            "AEDECOD": ["TEST1", "TEST2", "TESTPT3"],
        }
    )

    operation_params.dataframe = invalid_df
    operation_params.domain = "AE"
    operation_params.meddra_path = installed_meddra_dictionaries["meddra_path"]
    result = MedDRATermReferencesValidator(
        operation_params, invalid_df, cache, data_service
    ).execute()
    assert operation_params.operation_id in result
    assert result[operation_params.operation_id].equals(pd.Series([False, False, True]))
