from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
import pandas as pd
import pytest
from typing import List

from cdisc_rules_engine.models.operation_params import OperationParams

from cdisc_rules_engine.operations.get_codelist_attributes import (
    CodeListAttributes,
)
from cdisc_rules_engine.services.cache import InMemoryCacheService

from cdisc_rules_engine.services.data_services import LocalDataService

test_set1 = (
    ["sdtmct-2020-03-27"],
    {
        "STUDYID": ["CDISC001", "CDISC001", "CDISC001", "CDISC001", "CDISC001"],
        "DOMAIN": ["TS", "TS", "TS", "TS", "TS"],
        "TSSEQ": [1, 1, 1, 1, 2],
        "TSVALCD": ["C49487x", "C49487", "C25473x", "", ""],
        "TSVCDREF": ["CDISC CT", "CDISC CT", "CDISC CT", "SNOMED", "ISO 8601"],
        "TSVCDVER": ["2020-03-27", "2020-03-27", "2020-03-27", "", ""],
    },
    [
        {
            "package": "sdtmct-2020-03-27",
            "C49487": {"extensible": False, "allowed_terms": ["A", "B", "C"]},
            "C25473": {"extensible": False, "allowed_terms": ["X", "Y", "Z"]},
            "C141663": {"extensible": False, "allowed_terms": []},
        },
        {
            "package": "sdtmct-2022-12-16",
            "C141657": {"extensible": False, "allowed_terms": ["A", "B", "C"]},
            "C141656": {"extensible": False, "allowed_terms": ["X", "Y", "Z"]},
            "C141663": {"extensible": False, "allowed_terms": []},
        },
    ],
    PandasDataset,
    {"C141663", "C25473", "C49487"},
)

test_set2 = (
    ["sdtmct-2022-12-16"],
    {
        "STUDYID": ["CDISC001", "CDISC001", "CDISC001", "CDISC001", "CDISC001"],
        "DOMAIN": ["TS", "TS", "TS", "TS", "TS"],
        "TSSEQ": [1, 1, 1, 1, 2],
        "TSVALCD": ["C49487", "C49487", "C25473", "C141657", "C141663"],
        "TSVCDREF": ["CDISC", "CDISC", "CDISC CT", "CDISC CT", "CDISC CT"],
        "TSVCDVER": [
            "2020-03-27",
            "2020-03-27",
            "2020-03-27",
            "2022-12-16",
            "2022-12-16",
        ],
    },
    [
        {
            "package": "sdtmct-2020-03-27",
            "C49487": {"extensible": False, "allowed_terms": ["A", "B", "C"]},
            "C25473": {"extensible": False, "allowed_terms": ["X", "Y", "Z"]},
            "C141663": {"extensible": False, "allowed_terms": []},
        },
        {
            "package": "sdtmct-2022-12-16",
            "C141657": {"extensible": False, "allowed_terms": ["A", "B", "C"]},
            "C141656": {"extensible": False, "allowed_terms": ["X", "Y", "Z"]},
            "C141663": {"extensible": False, "allowed_terms": []},
        },
    ],
    DaskDataset,
    {"C141656", "C141663", "C141657"},
)


@pytest.mark.parametrize(
    "ct_packages, ts_data, ct_data, dataset_type, ct_list", [test_set1, test_set2]
)
def test_get_codelist_attributes(
    operation_params: OperationParams,
    ct_packages,
    ts_data: dict,
    ct_data: list,
    dataset_type,
    ct_list,
):
    """
    Unit test for DataProcessor.get_column_order_from_library.
    Mocks cache call to return metadata.
    """
    # 1.0 set parameters
    operation_params.dataframe = dataset_type.from_dict(ts_data)
    operation_params.domain = "TS"
    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"
    operation_params.ct_attribute: str = "TSVALCD"
    operation_params.ct_version: str = "TSVCDVER"
    operation_params.target = "TSVCDREF"
    operation_params.ct_packages: list = ct_packages

    # 2.0 add CT data to cache
    cache = InMemoryCacheService.get_instance()
    library_metadata = LibraryMetadataContainer()
    for pkg in ct_data:
        cp = pkg.get("package")
        library_metadata.set_ct_package_metadata(cp, pkg)

    # 3.0 execute operation
    data_service = LocalDataService.get_instance(
        cache_service=cache, config=ConfigService()
    )

    operation = CodeListAttributes(
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )

    result: pd.DataFrame = operation.execute()

    variables: List[str] = ct_list
    expected: pd.Series = pd.Series(
        [
            variables,
            variables,
            variables,
            variables,
            variables,
        ]
    )
    assert result[operation_params.operation_id].equals(expected)
