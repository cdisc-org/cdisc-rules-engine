from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from cdisc_rules_engine.dataset_builders.contents_define_dataset_builder import (  # noqa: E501
    ContentsDefineDatasetBuilder,
)
from cdisc_rules_engine.services.data_services import DummyDataService
from cdisc_rules_engine.dummy_models.dummy_dataset import DummyDataset


test_set1 = (
    [
        {
            "define_dataset_name": "TS",
            "define_dataset_label": "Trial Summary",
            "define_dataset_location": "ts.xpt",
            "define_dataset_class": "TRIAL DESIGN",
            "define_dataset_structure": "One record per trial summary parameter value",
            "define_dataset_is_non_standard": "",
            "define_dataset_variables": [
                "STUDYID",
                "DOMAIN",
                "TSSEQ",
                "TSPARMCD",
                "TSPARM",
                "TSVAL",
            ],
        },
        {
            "define_dataset_name": "DI",
            "define_dataset_label": "Device Identifiers",
            "define_dataset_location": "di.xpt",
            "define_dataset_class": "SPECIAL PURPOSE",
            "define_dataset_structure": "One record per device identifier per device",
            "define_dataset_is_non_standard": "",
        },
        {
            "define_dataset_name": "DM",
            "define_dataset_label": "Demographics",
            "define_dataset_location": "dm.xpt",
            "define_dataset_class": "SPECIAL PURPOSE",
            "define_dataset_structure": "One record per subject",
            "define_dataset_is_non_standard": "",
        },
    ],
    {
        "datasets": [
            {
                "filename": "ts.xpt",
                "label": "Trial Summary",
                "domain": "TS",
                "variables": [
                    {
                        "name": "STUDYID",
                        "label": "Study Identifier",
                        "type": "Char",
                        "length": 12,
                    },
                    {
                        "name": "DOMAIN",
                        "label": "Domain Abbreviation",
                        "type": "Char",
                        "length": 2,
                    },
                    {
                        "name": "TSSEQ",
                        "label": "Sequence Number",
                        "type": "Num",
                        "length": 8,
                    },
                ],
                "records": {
                    "STUDYID": ["CDISC001", "CDISC001", "CDISC001"],
                    "DOMAIN": ["TS", "TS", "TS"],
                    "TSSEQ": [1, 1, 2],
                },
            },
            {
                "filename": "dm.xpt",
                "label": "Demographics",
                "domain": "DM",
                "variables": [
                    {
                        "name": "STUDYID",
                        "label": "Study Identifier",
                        "type": "Char",
                        "length": 12,
                    },
                    {
                        "name": "DOMAIN",
                        "label": "Domain Abbreviation",
                        "type": "Char",
                        "length": 2,
                    },
                    {
                        "name": "USUBJID",
                        "label": "Unique Subject Identifier",
                        "type": "Char",
                        "length": 20,
                    },
                    {
                        "name": "SUBJID",
                        "label": "Subject Identifier for the Study",
                        "type": "Char",
                        "length": 12,
                    },
                ],
                "records": {
                    "STUDYID": ["CES", "CES", "CES"],
                    "DOMAIN": ["DM", "DM", "DM"],
                    "USUBJID": [
                        "015246-076-0003-00003",
                        "015246-203-0003-00001",
                        "015246-300-0004-00002",
                    ],
                    "SUBJID": ["076000300003", "203000300001", "300000400002"],
                },
            },
        ],
        "standard": {"product": "sdtmig", "version": "3-4"},
        "codelists": ["sdtmct-2022-12-16"],
    },
    {
        "dataset_name": ["TS", "DM", None],
        "define_dataset_name": ["TS", "DM", "DI"],
    },
)

test_set2 = (
    [
        {
            "define_dataset_name": "TS",
            "define_dataset_label": "Trial Summary",
            "define_dataset_location": "ts.xpt",
            "define_dataset_class": "TRIAL DESIGN",
            "define_dataset_structure": "One record per trial summary parameter value",
            "define_dataset_is_non_standard": "",
            "define_dataset_variables": [
                "STUDYID",
                "DOMAIN",
                "TSSEQ",
                "TSPARMCD",
                "TSPARM",
                "TSVAL",
            ],
        },
        {
            "define_dataset_name": "DI",
            "define_dataset_label": "Device Identifiers",
            "define_dataset_location": "di.xpt",
            "define_dataset_class": "SPECIAL PURPOSE",
            "define_dataset_structure": "One record per device identifier per device",
            "define_dataset_is_non_standard": "",
        },
        {
            "define_dataset_name": "DM",
            "define_dataset_label": "Demographics",
            "define_dataset_location": "dm.xpt",
            "define_dataset_class": "SPECIAL PURPOSE",
            "define_dataset_structure": "One record per subject",
            "define_dataset_is_non_standard": "",
        },
    ],
    {
        "datasets": [
            {
                "filename": "ts.xpt",
                "label": "Trial Summary",
                "domain": "TS",
                "variables": [
                    {
                        "name": "STUDYID",
                        "label": "Study Identifier",
                        "type": "Char",
                        "length": 12,
                    },
                    {
                        "name": "DOMAIN",
                        "label": "Domain Abbreviation",
                        "type": "Char",
                        "length": 2,
                    },
                    {
                        "name": "TSSEQ",
                        "label": "Sequence Number",
                        "type": "Num",
                        "length": 8,
                    },
                ],
                "records": {
                    "STUDYID": ["CDISC001", "CDISC001", "CDISC001"],
                    "DOMAIN": ["TS", "TS", "TS"],
                    "TSSEQ": [1, 1, 2],
                },
            },
            {
                "filename": "cm.xpt",
                "label": "Common Medicine",
                "domain": "CM",
                "variables": [],
                "records": {},
            },
        ],
        "standard": {"product": "sdtmig", "version": "3-4"},
        "codelists": ["sdtmct-2022-12-16"],
    },
    {
        "dataset_name": ["TS", "CM", None, None],
        "define_dataset_name": ["TS", None, "DI", "DM"],
    },
)

test_set3 = (
    [],
    {
        "datasets": [
            {
                "filename": "ts.xpt",
                "label": "Trial Summary",
                "domain": "TS",
                "variables": [],
                "records": {},
            },
            {
                "filename": "dm.xpt",
                "label": "Demographics",
                "domain": "DM",
                "variables": [],
                "records": {},
            },
        ],
    },
    {
        "dataset_name": ["TS", "DM"],
        "define_dataset_name": [None, None],
    },
)

test_set4 = (
    [
        {
            "define_dataset_name": "TS",
            "define_dataset_label": "Trial Summary",
            "define_dataset_location": "ts.xpt",
            "define_dataset_class": "TRIAL DESIGN",
            "define_dataset_structure": "One record per trial summary parameter value",
            "define_dataset_is_non_standard": "",
            "define_dataset_variables": [
                "STUDYID",
                "DOMAIN",
                "TSSEQ",
                "TSPARMCD",
                "TSPARM",
                "TSVAL",
            ],
        },
        {
            "define_dataset_name": "DI",
            "define_dataset_label": "Device Identifiers",
            "define_dataset_location": "di.xpt",
            "define_dataset_class": "TRIAL DESIGN",
            "define_dataset_structure": "One record per trial summary parameter value",
            "define_dataset_is_non_standard": "",
            "define_dataset_variables": [
                "STUDYID",
                "DOMAIN",
                "TSSEQ",
                "TSPARMCD",
                "TSPARM",
                "TSVAL",
            ],
        },
        {
            "define_dataset_name": "DM",
            "define_dataset_label": "Demographics",
            "define_dataset_location": "dm.xpt",
            "define_dataset_class": "TRIAL DESIGN",
            "define_dataset_structure": "One record per trial summary parameter value",
            "define_dataset_is_non_standard": "",
            "define_dataset_variables": [
                "STUDYID",
                "DOMAIN",
                "TSSEQ",
                "TSPARMCD",
                "TSPARM",
                "TSVAL",
            ],
        },
    ],
    {},
    {
        "dataset_name": [None, None, None],
        "define_dataset_name": ["TS", "DI", "DM"],
    },
)

test_set5 = (
    [],
    {},
    {
        "dataset_name": [],
        "define_dataset_name": [],
    },
)


@pytest.mark.parametrize(
    "define_metadata, data_metadata, expected",
    [test_set1, test_set2, test_set3, test_set4, test_set5],
)
@patch(
    "cdisc_rules_engine.dataset_builders.base_dataset_builder."
    + "BaseDatasetBuilder.get_define_metadata"
)
def test_contents_define_dataset_builder(
    mock_get_define_metadata, define_metadata, data_metadata, expected
):
    mock_get_define_metadata.return_value = define_metadata
    kwargs = {}
    kwargs["datasets"] = data_metadata.get("datasets")
    expected = pd.DataFrame.from_dict(expected)
    datasets = [DummyDataset(data) for data in data_metadata.get("datasets", [])]

    result = ContentsDefineDatasetBuilder(
        rule=None,
        data_service=DummyDataService(
            MagicMock(), MagicMock(), MagicMock(), data=datasets
        ),
        cache_service=None,
        rule_processor=None,
        data_processor=None,
        dataset_path=None,
        datasets=data_metadata.get("datasets", {}),
        domain=None,
        define_xml_path=None,
        standard="sdtmig",
        standard_version="3-4",
        library_metadata=LibraryMetadataContainer(),
    ).build()
    col_names = ["dataset_name", "define_dataset_name"]
    assert result[col_names].equals(expected[col_names]) or (
        result.empty and expected.empty
    )


@pytest.mark.parametrize(
    "define_metadata, data_metadata, expected",
    [test_set1, test_set2, test_set3, test_set4, test_set5],
)
@patch(
    "cdisc_rules_engine.dataset_builders.base_dataset_builder."
    + "BaseDatasetBuilder.get_define_metadata"
)
def test_contents_define_dataset_columns(
    mock_get_define_metadata, define_metadata, data_metadata, expected
):
    mock_get_define_metadata.return_value = define_metadata
    kwargs = {}
    kwargs["datasets"] = data_metadata.get("datasets")
    expected = pd.DataFrame.from_dict(expected)
    datasets = [DummyDataset(data) for data in data_metadata.get("datasets", [])]

    result = ContentsDefineDatasetBuilder(
        rule=None,
        data_service=DummyDataService(
            MagicMock(), MagicMock(), MagicMock(), data=datasets
        ),
        cache_service=None,
        rule_processor=None,
        data_processor=None,
        dataset_path=None,
        datasets=data_metadata.get("datasets", {}),
        domain=None,
        define_xml_path=None,
        standard="sdtmig",
        standard_version="3-4",
        library_metadata=LibraryMetadataContainer(),
    ).build()
    exp_columns = result.columns.tolist()
    req_columns = [
        "dataset_size",
        "dataset_location",
        "dataset_name",
        "dataset_label",
        "define_dataset_name",
        "define_dataset_label",
        "define_dataset_location",
        "define_dataset_class",
        "define_dataset_structure",
        "define_dataset_is_non_standard",
        "define_dataset_variables",
    ]
    assert exp_columns == req_columns
