from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.operations.valid_codelist_dates import ValidCodelistDates
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
import pytest


@pytest.mark.parametrize(
    "standard, expected",
    [
        ("sdtmig", ["2022-09-30", "2022-12-16"]),
        ("adamig", ["2022-09-30", "2022-12-16", "2021-12-17", "2022-06-24"]),
        ("sendig", ["2014-09-26", "2014-12-19"]),
        ("cdashig", ["2014-09-26", "2015-03-27"]),
    ],
)
def test_variable_count(
    standard, expected, mock_data_service, operation_params: OperationParams
):
    valid_codelists = [
        "sdtmct-2022-09-30",
        "sdtmct-2022-12-16",
        "sendct-2014-09-26",
        "sendct-2014-12-19",
        "adamct-2021-12-17",
        "adamct-2022-06-24",
        "cdashct-2014-09-26",
        "cdashct-2015-03-27",
    ]
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    library_metadata = LibraryMetadataContainer(published_ct_packages=valid_codelists)
    operation_params.standard = standard
    result = ValidCodelistDates(
        operation_params,
        pd.DataFrame.from_dict({"test": [1, 2, 33]}),
        cache,
        mock_data_service,
        library_metadata,
    ).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert val == expected
