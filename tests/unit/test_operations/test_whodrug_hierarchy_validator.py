from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.operations.whodrug_hierarchy_validator import (
    WhodrugHierarchyValidator,
)
from cdisc_rules_engine.models.dictionaries.whodrug.whodrug_terms_factory import (
    WhoDrugTermsFactory,
)
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd

from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)
from cdisc_rules_engine.models.dictionaries.whodrug.whodrug_file_names import (
    WhodrugFileNames,
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
        operation_params, invalid_df, cache, data_service
    ).execute()
    assert operation_params.operation_id in result
    assert result[operation_params.operation_id].equals(
        pd.Series([True, True, False, False])
    )


def test_get_code_hierarchies(tmp_path, operation_params: OperationParams):
    """
    Unit test for WhoDrugHierarchyValidator.get_code_hierarchies method.
    """
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    # create temporary files
    ina_file = tmp_path / WhodrugFileNames.INA_FILE_NAME.value
    ina_file.write_text(
        "A      1ALIMENTARY TRACT AND METABOLISM\n"
        "C02AB  2STOMATOLOGICAL PREPARATIONS\n"
        "A01A   3STOMATOLOGICAL PREPARATIONS"
    )

    dda_file = tmp_path / WhodrugFileNames.DDA_FILE_NAME.value
    dda_file.write_text("000001010016C02AB  111*\n" "000001010014C02AB  074\n")

    dd_file = tmp_path / WhodrugFileNames.DD_FILE_NAME.value
    dd_file.write_text(
        "000001010016N  001      01 854METHYLDOPA\n"
        "000001010024T21MEX      01 041ALDOMET [METHYLDOPA]\n"
        "000001010032T21336      01 044PRESINOL    \n"
        "000001010040T19UGA      01 044DOPAMET             "
    )

    # run the factory
    factory = WhoDrugTermsFactory(data_service)
    terms: dict = factory.install_terms(str(tmp_path))
    operation = WhodrugHierarchyValidator(
        operation_params, pd.DataFrame(), cache, data_service
    )
    valid_hierarchies = operation.get_code_hierarchies(terms)
    print(valid_hierarchies)
    assert valid_hierarchies == {
        "ALDOMET [METHYLDOPA]/STOMATOLOGICAL PREPARATIONS/C02AB",
        "METHYLDOPA/STOMATOLOGICAL PREPARATIONS/C02AB",
    }
