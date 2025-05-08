from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.operations.valid_codelist_dates import (
    ValidCodelistDates,
)
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.services.cache.cache_service_factory import (
    CacheServiceFactory,
)
import pytest


@pytest.mark.parametrize(
    "standard, ct_package_types, expected",
    [
        ("sdtmig", None, ["2022-09-30", "2022-12-16", "2024-09-27"]),
        (
            "adamig",
            None,
            [
                "2021-12-17",
                "2022-06-24",
                "2022-09-30",
                "2022-12-16",
                "2024-09-27",
            ],
        ),
        ("sendig", None, ["2014-09-26", "2014-12-19"]),
        ("cdashig", None, ["2014-09-26", "2015-03-27"]),
        (
            "usdm",
            None,
            ["2022-09-30", "2022-12-16", "2022-12-17", "2024-09-27"],
        ),
        (
            "sdtmig",
            ["SDTM", "CDASH"],
            [
                "2014-09-26",
                "2015-03-27",
                "2022-09-30",
                "2022-12-16",
                "2024-09-27",
            ],
        ),
        ("usdm", ["DDF"], ["2022-12-17", "2024-09-27"]),
    ],
)
def test_valid_codelist_dates(
    standard,
    expected,
    ct_package_types,
    mock_data_service,
    operation_params: OperationParams,
):
    valid_codelists = [
        "sdtmct-2022-09-30",
        "sdtmct-2022-12-16",
        "sdtmct-2024-09-27",
        "sendct-2014-09-26",
        "sendct-2014-12-19",
        "adamct-2021-12-17",
        "adamct-2022-06-24",
        "cdashct-2014-09-26",
        "cdashct-2015-03-27",
        "ddfct-2022-12-17",
        "ddfct-2024-09-27",
    ]
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    library_metadata = LibraryMetadataContainer(published_ct_packages=valid_codelists)
    operation_params.standard = standard
    operation_params.ct_package_types = ct_package_types
    result = ValidCodelistDates(
        operation_params,
        PandasDataset.from_dict({"test": [1, 2, 33]}),
        cache,
        mock_data_service,
        library_metadata,
    ).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert val == expected
