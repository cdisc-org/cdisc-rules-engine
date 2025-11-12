from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
import pandas as pd
import pytest

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
            "submission_lookup": {
                "N": {"codelist": "C49487", "term": "C49487"},
                "Y": {"codelist": "C25473", "term": "C25473"},
                "MAYBE": {"codelist": "C141663", "term": "C141663"},
            },
            "C49487": {
                "extensible": False,
                "preferredTerm": "No",
                "submissionValue": "N",
                "terms": [
                    {
                        "conceptId": "C49487",
                        "submissionValue": "N",
                        "preferredTerm": "No",
                    }
                ],
            },
            "C25473": {
                "extensible": False,
                "preferredTerm": "Yes",
                "submissionValue": "Y",
                "terms": [
                    {
                        "conceptId": "C25473",
                        "submissionValue": "Y",
                        "preferredTerm": "Yes",
                    }
                ],
            },
            "C141663": {
                "extensible": False,
                "preferredTerm": "Maybe",
                "submissionValue": "MAYBE",
                "terms": [
                    {
                        "conceptId": "C141663",
                        "submissionValue": "MAYBE",
                        "preferredTerm": "Maybe",
                    }
                ],
            },
        },
        {
            "package": "sdtmct-2022-12-16",
            "submission_lookup": {
                "A": {"codelist": "C141657", "term": "C141657"},
                "B": {"codelist": "C141656", "term": "C141656"},
                "C": {"codelist": "C141663", "term": "C141663"},
            },
            "C141657": {
                "extensible": False,
                "preferredTerm": "Option A",
                "submissionValue": "A",
                "terms": [
                    {
                        "conceptId": "C141657",
                        "submissionValue": "A",
                        "preferredTerm": "Option A",
                    }
                ],
            },
            "C141656": {
                "extensible": False,
                "preferredTerm": "Option B",
                "submissionValue": "B",
                "terms": [
                    {
                        "conceptId": "C141656",
                        "submissionValue": "B",
                        "preferredTerm": "Option B",
                    }
                ],
            },
            "C141663": {
                "extensible": False,
                "preferredTerm": "Option C",
                "submissionValue": "C",
                "terms": [
                    {
                        "conceptId": "C141663",
                        "submissionValue": "C",
                        "preferredTerm": "Option C",
                    }
                ],
            },
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
            "submission_lookup": {
                "N": {"codelist": "C49487", "term": "C49487"},
                "Y": {"codelist": "C25473", "term": "C25473"},
                "MAYBE": {"codelist": "C141663", "term": "C141663"},
            },
            "C49487": {
                "extensible": False,
                "preferredTerm": "No",
                "submissionValue": "N",
                "terms": [
                    {
                        "conceptId": "C49487",
                        "submissionValue": "N",
                        "preferredTerm": "No",
                    }
                ],
            },
            "C25473": {
                "extensible": False,
                "preferredTerm": "Yes",
                "submissionValue": "Y",
                "terms": [
                    {
                        "conceptId": "C25473",
                        "submissionValue": "Y",
                        "preferredTerm": "Yes",
                    }
                ],
            },
            "C141663": {
                "extensible": False,
                "preferredTerm": "Maybe",
                "submissionValue": "MAYBE",
                "terms": [
                    {
                        "conceptId": "C141663",
                        "submissionValue": "MAYBE",
                        "preferredTerm": "Maybe",
                    }
                ],
            },
        },
        {
            "package": "sdtmct-2022-12-16",
            "submission_lookup": {
                "A": {"codelist": "C141657", "term": "C141657"},
                "B": {"codelist": "C141656", "term": "C141656"},
                "C": {"codelist": "C141663", "term": "C141663"},
            },
            "C141657": {
                "extensible": False,
                "preferredTerm": "Option A",
                "submissionValue": "A",
                "terms": [
                    {
                        "conceptId": "C141657",
                        "submissionValue": "A",
                        "preferredTerm": "Option A",
                    }
                ],
            },
            "C141656": {
                "extensible": False,
                "preferredTerm": "Option B",
                "submissionValue": "B",
                "terms": [
                    {
                        "conceptId": "C141656",
                        "submissionValue": "B",
                        "preferredTerm": "Option B",
                    }
                ],
            },
            "C141663": {
                "extensible": False,
                "preferredTerm": "Option C",
                "submissionValue": "C",
                "terms": [
                    {
                        "conceptId": "C141663",
                        "submissionValue": "C",
                        "preferredTerm": "Option C",
                    }
                ],
            },
        },
    ],
    PandasDataset,
    {"C141656", "C141663", "C141657"},
)

test_set3 = (
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
            "submission_lookup": {
                "N": {"codelist": "C49487", "term": "C49487"},
                "Y": {"codelist": "C25473", "term": "C25473"},
                "MAYBE": {"codelist": "C141663", "term": "C141663"},
            },
            "C49487": {
                "extensible": False,
                "preferredTerm": "No",
                "submissionValue": "N",
                "terms": [
                    {
                        "conceptId": "C49487",
                        "submissionValue": "N",
                        "preferredTerm": "No",
                    }
                ],
            },
            "C25473": {
                "extensible": False,
                "preferredTerm": "Yes",
                "submissionValue": "Y",
                "terms": [
                    {
                        "conceptId": "C25473",
                        "submissionValue": "Y",
                        "preferredTerm": "Yes",
                    }
                ],
            },
            "C141663": {
                "extensible": False,
                "preferredTerm": "Maybe",
                "submissionValue": "MAYBE",
                "terms": [
                    {
                        "conceptId": "C141663",
                        "submissionValue": "MAYBE",
                        "preferredTerm": "Maybe",
                    }
                ],
            },
        },
        {
            "package": "sdtmct-2022-12-16",
            "submission_lookup": {
                "A": {"codelist": "C141657", "term": "C141657"},
                "B": {"codelist": "C141656", "term": "C141656"},
                "C": {"codelist": "C141663", "term": "C141663"},
            },
            "C141657": {
                "extensible": False,
                "preferredTerm": "Option A",
                "submissionValue": "A",
                "terms": [
                    {
                        "conceptId": "C141657",
                        "submissionValue": "A",
                        "preferredTerm": "Option A",
                    }
                ],
            },
            "C141656": {
                "extensible": False,
                "preferredTerm": "Option B",
                "submissionValue": "B",
                "terms": [
                    {
                        "conceptId": "C141656",
                        "submissionValue": "B",
                        "preferredTerm": "Option B",
                    }
                ],
            },
            "C141663": {
                "extensible": False,
                "preferredTerm": "Option C",
                "submissionValue": "C",
                "terms": [
                    {
                        "conceptId": "C141663",
                        "submissionValue": "C",
                        "preferredTerm": "Option C",
                    }
                ],
            },
        },
    ],
    DaskDataset,
    {"C141663", "C25473", "C49487"},
)

test_set4 = (
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
            "submission_lookup": {
                "N": {"codelist": "C49487", "term": "C49487"},
                "Y": {"codelist": "C25473", "term": "C25473"},
                "MAYBE": {"codelist": "C141663", "term": "C141663"},
            },
            "C49487": {
                "extensible": False,
                "preferredTerm": "No",
                "submissionValue": "N",
                "terms": [
                    {
                        "conceptId": "C49487",
                        "submissionValue": "N",
                        "preferredTerm": "No",
                    }
                ],
            },
            "C25473": {
                "extensible": False,
                "preferredTerm": "Yes",
                "submissionValue": "Y",
                "terms": [
                    {
                        "conceptId": "C25473",
                        "submissionValue": "Y",
                        "preferredTerm": "Yes",
                    }
                ],
            },
            "C141663": {
                "extensible": False,
                "preferredTerm": "Maybe",
                "submissionValue": "MAYBE",
                "terms": [
                    {
                        "conceptId": "C141663",
                        "submissionValue": "MAYBE",
                        "preferredTerm": "Maybe",
                    }
                ],
            },
        },
        {
            "package": "sdtmct-2022-12-16",
            "submission_lookup": {
                "A": {"codelist": "C141657", "term": "C141657"},
                "B": {"codelist": "C141656", "term": "C141656"},
                "C": {"codelist": "C141663", "term": "C141663"},
            },
            "C141657": {
                "extensible": False,
                "preferredTerm": "Option A",
                "submissionValue": "A",
                "terms": [
                    {
                        "conceptId": "C141657",
                        "submissionValue": "A",
                        "preferredTerm": "Option A",
                    }
                ],
            },
            "C141656": {
                "extensible": False,
                "preferredTerm": "Option B",
                "submissionValue": "B",
                "terms": [
                    {
                        "conceptId": "C141656",
                        "submissionValue": "B",
                        "preferredTerm": "Option B",
                    }
                ],
            },
            "C141663": {
                "extensible": False,
                "preferredTerm": "Option C",
                "submissionValue": "C",
                "terms": [
                    {
                        "conceptId": "C141663",
                        "submissionValue": "C",
                        "preferredTerm": "Option C",
                    }
                ],
            },
        },
    ],
    DaskDataset,
    {"C141656", "C141663", "C141657"},
)


@pytest.mark.parametrize(
    "ct_packages, ts_data, ct_data, dataset_type, ct_list",
    [test_set1, test_set2, test_set3, test_set4],
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
    Unit test for CodeListAttributes operation.
    Tests that the operation returns the correct term codes based on CT version.
    """
    # 1.0 set parameters
    operation_params.dataframe = dataset_type.from_dict(ts_data)
    operation_params.domain = "TS"
    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"
    operation_params.ct_attribute = "Term CCODE"  # Changed from TSVALCD
    operation_params.ct_version = "TSVCDVER"
    operation_params.target = "TSVCDREF"
    operation_params.ct_packages = ct_packages

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

    result = operation.execute()

    # Extract the operation_id column which contains the sets
    result_series = result[operation_params.operation_id]

    # Expected: Each row gets the ct_list only if its version matches ct_packages
    # For test_set1 and test_set3: All rows with version 2020-03-27 should get ct_list
    # For test_set2 and test_set4: Only rows 3 and 4 with version 2022-12-16 should get ct_list

    if ct_packages == ["sdtmct-2020-03-27"]:
        # Rows 0, 1, 2 have version 2020-03-27 (match)
        # Rows 3, 4 have empty version (no match)
        expected = pd.Series([ct_list, ct_list, ct_list, set(), set()])
    else:  # ct_packages == ["sdtmct-2022-12-16"]
        # Rows 0, 1, 2 have version 2020-03-27 (no match)
        # Rows 3, 4 have version 2022-12-16 (match)
        expected = pd.Series([set(), set(), set(), ct_list, ct_list])

    # Compare the series - each element should already be a set
    assert len(result_series) == len(expected)
    for i in range(len(result_series)):
        # Both result_series.iloc[i] and expected.iloc[i] should be sets already
        assert (
            result_series.iloc[i] == expected.iloc[i]
        ), f"Row {i}: {result_series.iloc[i]} != {expected.iloc[i]}"
