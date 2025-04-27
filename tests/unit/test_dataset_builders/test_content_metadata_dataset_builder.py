import pytest
from cdisc_rules_engine.dataset_builders.content_metadata_dataset_builder import (
    ContentMetadataDatasetBuilder,
)
from unittest.mock import MagicMock
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.services.data_services import DummyDataService
from cdisc_rules_engine.dummy_models.dummy_dataset import DummyDataset
from cdisc_rules_engine.models.rule_conditions import ConditionCompositeFactory
from conftest import mock_data_service
from cdisc_rules_engine.utilities.rule_processor import RuleProcessor
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
from cdisc_rules_engine.models.dataset import PandasDataset
import pandas as pd

test_data = {
    "datasets": [
        {
            "filename": "qscg.xpt",
            "label": "Clinical Global Impressions",
            "domain": "QS",
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
                    "length": 4,
                },
                {
                    "name": "USUBJID",
                    "label": "Unique Subject Identifier",
                    "type": "Char",
                    "length": 8,
                },
                {
                    "name": "QSSEQ",
                    "label": "Sequence Number",
                    "type": "Num",
                    "length": 8,
                },
                {
                    "name": "QSTESTCD",
                    "label": "Question Short Name",
                    "type": "Char",
                    "length": 7,
                },
                {
                    "name": "QSTEST",
                    "label": "Question Name",
                    "type": "Char",
                    "length": 40,
                },
                {
                    "name": "QSCAT",
                    "label": "Category of Question",
                    "type": "Char",
                    "length": 5,
                },
                {
                    "name": "QSORRES",
                    "label": "Finding in Original Units",
                    "type": "Char",
                    "length": 23,
                },
                {
                    "name": "QSSTRESC",
                    "label": "Character Result/Finding in Std Format",
                    "type": "Char",
                    "length": 20,
                },
                {
                    "name": "QSSTRESN",
                    "label": "Numeric Finding in Standard Units",
                    "type": "Num",
                    "length": 8,
                },
                {
                    "name": "QSLOBXFL",
                    "label": "Last Observation Before Exposure Flag",
                    "type": "Char",
                    "length": 1,
                },
                {
                    "name": "VISITNUM",
                    "label": "Visit Number",
                    "type": "Num",
                    "length": 8,
                },
                {"name": "VISIT", "label": "Visit Name", "type": "Char", "length": 200},
                {
                    "name": "QSDTC",
                    "label": "Date/Time of Finding",
                    "type": "Char",
                    "length": 10,
                },
                {
                    "name": "QSDY",
                    "label": "Study Day of Finding",
                    "type": "Num",
                    "length": 8,
                },
            ],
            "records": {
                "STUDYID": ["CDISC01"],
                "DOMAIN": ["QS"],
                "USUBJID": ["CDISC01.100008"],
                "QSSEQ": [1],
                "QSTESTCD": ["CGI0201"],
                "QSTEST": ["CGI02-Severity"],
                "QSCAT": ["CGI"],
                "QSORRES": ["Moderate"],
                "QSSTRESC": ["4"],
                "QSSTRESN": [4],
                "QSLOBXFL": ["Y"],
                "VISITNUM": [1],
                "VISIT": ["WEEK 1"],
                "QSDTC": ["2003-04-15"],
                "QSDY": [1],
            },
        },
        {
            "filename": "qspg.xpt",
            "label": "Patient Global Impressions",
            "domain": "QS",
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
                    "length": 4,
                },
                {
                    "name": "USUBJID",
                    "label": "Unique Subject Identifier",
                    "type": "Char",
                    "length": 8,
                },
                {
                    "name": "QSSEQ",
                    "label": "Sequence Number",
                    "type": "Num",
                    "length": 8,
                },
                {
                    "name": "QSTESTCD",
                    "label": "Question Short Name",
                    "type": "Char",
                    "length": 7,
                },
                {
                    "name": "QSTEST",
                    "label": "Question Name",
                    "type": "Char",
                    "length": 40,
                },
                {
                    "name": "QSCAT",
                    "label": "Category of Question",
                    "type": "Char",
                    "length": 5,
                },
                {
                    "name": "QSORRES",
                    "label": "Finding in Original Units",
                    "type": "Char",
                    "length": 23,
                },
                {
                    "name": "QSSTRESC",
                    "label": "Character Result/Finding in Std Format",
                    "type": "Char",
                    "length": 20,
                },
                {
                    "name": "QSSTRESN",
                    "label": "Numeric Finding in Standard Units",
                    "type": "Num",
                    "length": 8,
                },
                {
                    "name": "QSLOBXFL",
                    "label": "Last Observation Before Exposure Flag",
                    "type": "Char",
                    "length": 1,
                },
                {
                    "name": "VISITNUM",
                    "label": "Visit Number",
                    "type": "Num",
                    "length": 8,
                },
                {"name": "VISIT", "label": "Visit Name", "type": "Char", "length": 200},
                {
                    "name": "QSDTC",
                    "label": "Date/Time of Finding",
                    "type": "Char",
                    "length": 10,
                },
                {
                    "name": "QSDY",
                    "label": "Study Day of Finding",
                    "type": "Num",
                    "length": 8,
                },
            ],
            "records": {
                "STUDYID": ["CDISC01"],
                "DOMAIN": ["QS"],
                "USUBJID": ["CDISC01.100008"],
                "QSSEQ": [1],
                "QSTESTCD": ["PGI0201"],
                "QSTEST": ["PGI02-Severity"],
                "QSCAT": ["CGI"],
                "QSORRES": ["Moderate"],
                "QSSTRESC": ["4"],
                "QSSTRESN": [4],
                "QSLOBXFL": ["Y"],
                "VISITNUM": [1],
                "VISIT": ["WEEK 1"],
                "QSDTC": ["2003-04-15"],
                "QSDY": [1],
            },
        },
    ],
    "codelists": [],
}


@pytest.mark.parametrize(
    "conditions",
    [
        {
            "not": {
                "any": [
                    {
                        "value": {
                            "target": "dataset_label",
                            "comparator": "Adverse Events",
                        },
                        "operator": "equal_to",
                    },
                    {
                        "all": [
                            {
                                "value": {
                                    "target": "dataset_size",
                                    "unit": "MB",
                                    "comparator": 5,
                                },
                                "operator": "less_than",
                            },
                        ]
                    },
                ]
            }
        },
    ],
)
def test_ContentMetadataDatasetBuilder_split_datasets(conditions):
    rule: dict = {
        "conditions": ConditionCompositeFactory.get_condition_composite(conditions),
    }
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    data_metadata = test_data
    datasets = [DummyDataset(data) for data in data_metadata.get("datasets", [])]
    expected_output = {
        "dataset_size": 1000,
        "dataset_location": "qscg.xpt",
        "dataset_name": "QSCG",
        "dataset_label": "Clinical Global Impressions",
        "record_count": 1,
    }
    expected = PandasDataset(
        pd.DataFrame.from_dict([expected_output], orient="columns")
    )
    result = ContentMetadataDatasetBuilder(
        rule=rule,
        data_service=DummyDataService(
            MagicMock(), MagicMock(), MagicMock(), data=datasets
        ),
        cache_service=None,
        rule_processor=processor,
        data_processor=None,
        dataset_path=test_data["datasets"][0]["filename"],
        datasets=test_data.get("datasets", {}),
        dataset_metadata=test_data["datasets"][0],
        define_xml_path=None,
        standard="sdtmig",
        standard_version="3-4",
        standard_substandard=None,
        library_metadata=LibraryMetadataContainer(),
    ).build()
    expected_output2 = {
        "dataset_size": 1000,
        "dataset_location": "qspg.xpt",
        "dataset_name": "QSPG",
        "dataset_label": "Patient Global Impressions",
        "record_count": 1,
    }
    expected2 = PandasDataset(
        pd.DataFrame.from_dict([expected_output2], orient="columns")
    )
    result2 = ContentMetadataDatasetBuilder(
        rule=rule,
        data_service=DummyDataService(
            MagicMock(), MagicMock(), MagicMock(), data=datasets
        ),
        cache_service=None,
        rule_processor=processor,
        data_processor=None,
        dataset_path=test_data["datasets"][1]["filename"],
        datasets=test_data.get("datasets", {}),
        dataset_metadata=test_data["datasets"][1],
        define_xml_path=None,
        standard="sdtmig",
        standard_version="3-4",
        standard_substandard=None,
        library_metadata=LibraryMetadataContainer(),
    ).build()
    assert result.equals(expected)
    assert result2.equals(expected2)
