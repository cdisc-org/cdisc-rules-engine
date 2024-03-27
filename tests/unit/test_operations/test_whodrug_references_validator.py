from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.whodrug_references_validator import (
    WhodrugReferencesValidator,
)
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd

from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)
import pytest


@pytest.mark.parametrize("dataset_type", [(PandasDataset), (DaskDataset)])
def test_valid_whodrug_references(
    installed_whodrug_dictionaries: dict,
    operation_params: OperationParams,
    dataset_type,
):
    """
    Unit test for valid_whodrug_references function.
    """
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    # create a dataset where 2 rows reference invalid terms
    invalid_df = dataset_type.from_dict(
        {
            "DOMAIN": [
                "AE",
                "AE",
                "AE",
                "AE",
            ],
            "AEINA": ["A", "A01", "A01AC", "A01AD"],
        }
    )

    operation_params.dataframe = invalid_df
    operation_params.target = "AEINA"
    operation_params.domain = "AE"
    operation_params.whodrug_path = installed_whodrug_dictionaries["whodrug_path"]
    result = WhodrugReferencesValidator(
        operation_params, invalid_df, cache, data_service
    ).execute()
    assert operation_params.operation_id in result
    assert result[operation_params.operation_id].equals(
        pd.Series([True, True, False, False])
    )
