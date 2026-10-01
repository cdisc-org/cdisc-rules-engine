from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
import pytest

from cdisc_rules_engine.models.operation_params import OperationParams

from cdisc_rules_engine.operations.get_codelist_attributes import (
    CodeListAttributes,
)
from cdisc_rules_engine.services.cache import InMemoryCacheService

from cdisc_rules_engine.services.data_services import LocalDataService

test_set1 = (
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
            "codelists": [
                {
                    "conceptId": "C49487",
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
                {
                    "conceptId": "C25473",
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
                {
                    "conceptId": "C141663",
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
            ],
        },
        {
            "package": "sdtmct-2022-12-16",
            "codelists": [
                {
                    "conceptId": "C141657",
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
                {
                    "conceptId": "C141656",
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
                {
                    "conceptId": "C141663",
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
            ],
        },
    ],
    PandasDataset,
    [
        {"C141663", "C25473", "C49487"},
        {"C141663", "C25473", "C49487"},
        {"C141663", "C25473", "C49487"},
        set(),
        set(),
    ],
)

test_set2 = (
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
            "codelists": [
                {
                    "conceptId": "C49487",
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
                {
                    "conceptId": "C25473",
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
                {
                    "conceptId": "C141663",
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
            ],
        },
        {
            "package": "sdtmct-2022-12-16",
            "codelists": [
                {
                    "conceptId": "C141657",
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
                {
                    "conceptId": "C141656",
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
                {
                    "conceptId": "C141663",
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
            ],
        },
    ],
    PandasDataset,
    [
        {"C141663", "C25473", "C49487"},  # Row 0: sdtmct-2020-03-27
        {"C141663", "C25473", "C49487"},  # Row 1: sdtmct-2020-03-27
        {"C141663", "C25473", "C49487"},  # Row 2: sdtmct-2020-03-27
        {"C141656", "C141663", "C141657"},  # Row 3: sdtmct-2022-12-16
        {"C141656", "C141663", "C141657"},  # Row 4: sdtmct-2022-12-16
    ],
)

test_set3 = (
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
            "codelists": [
                {
                    "conceptId": "C49487",
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
                {
                    "conceptId": "C25473",
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
                {
                    "conceptId": "C141663",
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
            ],
        },
        {
            "package": "sdtmct-2022-12-16",
            "codelists": [
                {
                    "conceptId": "C141657",
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
                {
                    "conceptId": "C141656",
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
                {
                    "conceptId": "C141663",
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
            ],
        },
    ],
    DaskDataset,
    [
        {"C141663", "C25473", "C49487"},
        {"C141663", "C25473", "C49487"},
        {"C141663", "C25473", "C49487"},
        set(),
        set(),
    ],
)

test_set4 = (
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
            "codelists": [
                {
                    "conceptId": "C49487",
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
                {
                    "conceptId": "C25473",
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
                {
                    "conceptId": "C141663",
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
            ],
        },
        {
            "package": "sdtmct-2022-12-16",
            "codelists": [
                {
                    "conceptId": "C141657",
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
                {
                    "conceptId": "C141656",
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
                {
                    "conceptId": "C141663",
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
            ],
        },
    ],
    DaskDataset,
    [
        {"C141663", "C25473", "C49487"},
        {"C141663", "C25473", "C49487"},
        {"C141663", "C25473", "C49487"},
        {"C141656", "C141663", "C141657"},
        {"C141656", "C141663", "C141657"},
    ],
)


@pytest.mark.parametrize(
    "ts_data, ct_data, dataset_type, expected_results",
    [test_set1, test_set2, test_set3, test_set4],
)
def test_get_codelist_attributes(
    operation_params: OperationParams,
    ts_data: dict,
    ct_data: list,
    dataset_type,
    expected_results: list,
):
    operation_params.dataframe = dataset_type.from_dict(ts_data)
    operation_params.domain = "TS"
    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"
    operation_params.ct_attribute = "Term CCODE"
    operation_params.ct_version = "TSVCDVER"
    operation_params.target = "TSVCDREF"
    cache = InMemoryCacheService.get_instance()
    library_metadata = LibraryMetadataContainer()
    for pkg in ct_data:
        cp = pkg.get("package")
        library_metadata.set_ct_package_metadata(cp, pkg)
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
    result_series = result[operation_params.operation_id]
    assert len(result_series) == len(
        expected_results
    ), f"Length mismatch: got {len(result_series)}, expected {len(expected_results)}"
    for i in range(len(result_series)):
        actual = result_series.iloc[i]
        expected = expected_results[i]
        assert actual == expected, (
            f"Row {i} mismatch:\n"
            f"  Expected: {expected}\n"
            f"  Got: {actual}\n"
            f"  TSVCDREF: {ts_data['TSVCDREF'][i]}\n"
            f"  TSVCDVER: {ts_data['TSVCDVER'][i]}"
        )


STATIC_DOMAIN_DATA = {
    "STUDYID": ["CDISC001", "CDISC001", "CDISC001"],
    "DOMAIN": ["AE", "CM", "ZZ"],
    "AESEQ": [1, 2, 3],
}

STATIC_CT_2020 = {
    "package": "sdtmct-2020-03-27",
    "codelists": [
        {
            "conceptId": "C66734",
            "extensible": "true",
            "name": "SDTM Domain Abbreviation",
            "submissionValue": "DOMAIN",
            "terms": [
                {
                    "conceptId": "C49562",
                    "submissionValue": "AE",
                    "preferredTerm": "Adverse Event Domain",
                },
                {
                    "conceptId": "C49568",
                    "submissionValue": "CM",
                    "preferredTerm": "Concomitant Medication Domain",
                },
            ],
        },
        {
            "conceptId": "C66742",
            "extensible": "false",
            "name": "No Yes Response",
            "submissionValue": "NY",
            "terms": [
                {
                    "conceptId": "C49487",
                    "submissionValue": "N",
                    "preferredTerm": "No",
                },
                {
                    "conceptId": "C49488",
                    "submissionValue": "Y",
                    "preferredTerm": "Yes",
                },
            ],
        },
    ],
}

STATIC_CT_2022 = {
    "package": "sdtmct-2022-12-16",
    "codelists": [
        {
            "conceptId": "C66734",
            "extensible": "true",
            "name": "SDTM Domain Abbreviation",
            "submissionValue": "DOMAIN",
            "terms": [
                {
                    "conceptId": "C49562",
                    "submissionValue": "AE",
                    "preferredTerm": "Adverse Event Domain",
                },
                {
                    "conceptId": "C49568",
                    "submissionValue": "CM",
                    "preferredTerm": "Concomitant Medication Domain",
                },
                {
                    "conceptId": "C117755",
                    "submissionValue": "BE",
                    "preferredTerm": "Biospecimen Events Domain",
                },
            ],
        },
    ],
}

DOMAIN_CODES_2020 = {"C49562", "C49568"}
DOMAIN_CODES_2022 = {"C49562", "C49568", "C117755"}
DOMAIN_CODELIST = [{"Codelist Code": "C66734"}]

static_test_cases = [
    (
        "loaded_package_with_codelist_condition",
        [STATIC_CT_2020],
        None,
        "Term CCODE",
        DOMAIN_CODELIST,
        DOMAIN_CODES_2020,
    ),
    (
        "union_of_loaded_packages",
        [STATIC_CT_2020, STATIC_CT_2022],
        None,
        "Term CCODE",
        DOMAIN_CODELIST,
        DOMAIN_CODES_2020 | DOMAIN_CODES_2022,
    ),
    (
        "literal_version_selects_one_package",
        [STATIC_CT_2020, STATIC_CT_2022],
        "2022-12-16",
        "Term CCODE",
        DOMAIN_CODELIST,
        DOMAIN_CODES_2022,
    ),
    (
        "literal_full_package_name",
        [STATIC_CT_2020, STATIC_CT_2022],
        "sdtmct-2020-03-27",
        "Term CCODE",
        DOMAIN_CODELIST,
        DOMAIN_CODES_2020,
    ),
    (
        "underscore_condition_key",
        [STATIC_CT_2020],
        None,
        "Term CCODE",
        [{"Codelist_Code": "C66734"}],
        DOMAIN_CODES_2020,
    ),
    (
        "no_conditions_returns_all_terms",
        [STATIC_CT_2020],
        None,
        "Term CCODE",
        None,
        DOMAIN_CODES_2020 | {"C49487", "C49488"},
    ),
    (
        "term_level_condition",
        [STATIC_CT_2020],
        None,
        "Term CCODE",
        [{"Codelist Code": "C66742"}, {"Term Value": "Y"}],
        {"C49488"},
    ),
    (
        "codelist_attribute_with_extensible_condition",
        [STATIC_CT_2020],
        None,
        "Codelist CCODE",
        [{"Extensible": True}],
        {"C66734"},
    ),
    (
        "no_matching_codelist",
        [STATIC_CT_2020],
        None,
        "Term CCODE",
        [{"Codelist Code": "C99999"}],
        set(),
    ),
    (
        "no_packages_loaded",
        [],
        None,
        "Term CCODE",
        DOMAIN_CODELIST,
        set(),
    ),
]


def _run_static(
    operation_params: OperationParams,
    dataset_type,
    loaded_packages: list,
    ct_version,
    ct_attribute: str,
    ct_conditions,
):
    operation_params.dataframe = dataset_type.from_dict(STATIC_DOMAIN_DATA)
    operation_params.domain = "AE"
    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"
    operation_params.ct_attribute = ct_attribute
    operation_params.ct_version = ct_version
    operation_params.target = "DOMAIN"
    operation_params.ct_conditions = ct_conditions
    cache = InMemoryCacheService.get_instance()
    library_metadata = LibraryMetadataContainer(ct_package_metadata={})
    for pkg in loaded_packages:
        library_metadata.set_ct_package_metadata(pkg["package"], pkg)
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
    return operation.execute()[operation_params.operation_id]


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
@pytest.mark.parametrize(
    "loaded_packages, ct_version, ct_attribute, ct_conditions, expected",
    [case[1:] for case in static_test_cases],
    ids=[case[0] for case in static_test_cases],
)
def test_get_codelist_attributes_static(
    operation_params: OperationParams,
    dataset_type,
    loaded_packages: list,
    ct_version,
    ct_attribute: str,
    ct_conditions,
    expected: set,
):
    result_series = _run_static(
        operation_params,
        dataset_type,
        loaded_packages,
        ct_version,
        ct_attribute,
        ct_conditions,
    )
    assert len(result_series) == len(STATIC_DOMAIN_DATA["DOMAIN"])
    for i in range(len(result_series)):
        actual = result_series.iloc[i]
        assert actual == expected, (
            f"Row {i} mismatch:\n"
            f"  Expected: {expected}\n"
            f"  Got: {actual}\n"
            f"  DOMAIN: {STATIC_DOMAIN_DATA['DOMAIN'][i]}"
        )


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_get_codelist_attributes_static_unsupported_condition_key(
    operation_params: OperationParams, dataset_type
):
    with pytest.raises(ValueError, match="Unsupported ct_conditions key"):
        _run_static(
            operation_params,
            dataset_type,
            [STATIC_CT_2020],
            None,
            "Term CCODE",
            [{"Not A Field": "x"}],
        )
