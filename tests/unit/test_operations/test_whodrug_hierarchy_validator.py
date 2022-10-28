from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.operations.whodrug_hierarchy_validator import (
    WhodrugHierarchyValidator,
)
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd

from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)


def test_valid_whodrug_references(
    installed_whodrug_dictionaries: dict, operation_params: OperationParams
):
    """
    Unit test for valid_whodrug_references function.
    """
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    # create a dataset where 2 rows reference invalid terms
    invalid_df = pd.DataFrame.from_dict(
        {
            "DOMAIN": [
                "AE",
                "AE",
                "AE",
                "AE",
            ],
            "AEDECOD": ["DUMMYDRUG1", "DUMMYDRUG2", "DUMMYDRUG3", "DUMMYDRUG4"],
            "AECLAS": [
                "DUMMYATCTEXT4",
                "DUMMYATCTEXT4",
                "DUMMYATCTEXT4",
                "DUMMYATCTEXT4",
            ],
            "AECLASCD": ["C02AB", "C02AB", "R03AB", "R03CB"],
        }
    )

    operation_params.dataframe = invalid_df
    operation_params.target = "AEINA"
    operation_params.domain = "AE"
    operation_params.whodrug_path = installed_whodrug_dictionaries["whodrug_path"]
    result = WhodrugHierarchyValidator(
        operation_params, pd.DataFrame(), cache, data_service
    ).execute()
    assert operation_params.operation_id in result
    assert result[operation_params.operation_id].equals(
        pd.Series([True, True, False, False])
    )
