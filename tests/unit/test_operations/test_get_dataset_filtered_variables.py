from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
import pytest
import pandas as pd
from typing import List
from unittest.mock import patch

from cdisc_rules_engine.constants.classes import (
    GENERAL_OBSERVATIONS_CLASS,
    FINDINGS_ABOUT,
    EVENTS,
)
from cdisc_rules_engine.enums.variable_roles import VariableRoles
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.get_dataset_filtered_variables import (
    GetDatasetFilteredVariables,
)
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.config import ConfigService
from cdisc_rules_engine.services.data_readers import DataReaderFactory
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata

events_timing_test = (
    {
        "datasets": [
            {
                "_links": {"parentClass": {"title": "Events"}},
                "name": "AE",
                "datasetVariables": [
                    {"name": "USUBJID", "ordinal": 2},
                    {"name": "AESEQ", "ordinal": 3},
                    {"name": "AETERM", "ordinal": 4},
                    {
                        "name": "VISITNUM",
                        "ordinal": 17,
                        "role": VariableRoles.TIMING.value,
                    },
                    {
                        "name": "VISIT",
                        "ordinal": 18,
                        "role": VariableRoles.TIMING.value,
                    },
                ],
            }
        ],
        "classes": [
            {
                "name": "Events",
                "label": "Events",
                "classVariables": [
                    {"name": "--TERM", "ordinal": 1},
                    {"name": "--SEQ", "ordinal": 2},
                ],
            },
            {
                "name": GENERAL_OBSERVATIONS_CLASS,
                "label": GENERAL_OBSERVATIONS_CLASS,
                "classVariables": [
                    {
                        "name": "STUDYID",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 1,
                    },
                    {
                        "name": "DOMAIN",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 2,
                    },
                    {
                        "name": "USUBJID",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 3,
                    },
                    {
                        "name": "VISITNUM",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 17,
                    },
                    {
                        "name": "VISIT",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 18,
                    },
                    {
                        "name": "AETIMING",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 33,
                    },
                ],
            },
        ],
    },
    {
        "_links": {"model": {"href": "/mdr/sdtm/1-5"}},
        "classes": [
            {
                "name": "Events",
                "datasets": [
                    {
                        "name": "AE",
                        "label": "Adverse Events",
                        "datasetVariables": [
                            {"name": "AETEST", "ordinal": 1},
                            {"name": "AENEW", "ordinal": 2},
                            {
                                "name": "VISITNUM",
                                "ordinal": 3,
                                "role": VariableRoles.TIMING.value,
                            },
                            {
                                "name": "VISIT",
                                "ordinal": 4,
                                "role": VariableRoles.TIMING.value,
                            },
                        ],
                    }
                ],
            }
        ],
    },
    {
        "STUDYID": ["TEST_STUDY", "TEST_STUDY", "TEST_STUDY"],
        "DOMAIN": ["AE", "AE", "AE"],
        "USUBJID": ["SUBJ001", "SUBJ002", "SUBJ003"],
        "AETERM": ["Headache", "Nausea", "Fatigue"],
        "VISITNUM": [1, 2, 1],
        "VISIT": ["Day 1", "Day 7", "Day 1"],
    },
    {"name": "AE"},
    "role",
    "Timing",
    ["VISITNUM", "VISIT"],
)

events_identifier_test = (
    {
        "datasets": [
            {
                "_links": {"parentClass": {"title": "Events"}},
                "name": "AE",
                "datasetVariables": [
                    {"name": "USUBJID", "ordinal": 2},
                    {"name": "AESEQ", "ordinal": 3},
                    {"name": "AETERM", "ordinal": 4},
                ],
            }
        ],
        "classes": [
            {
                "name": "Events",
                "label": "Events",
                "classVariables": [
                    {"name": "--TERM", "ordinal": 1},
                    {"name": "--SEQ", "ordinal": 2},
                ],
            },
            {
                "name": GENERAL_OBSERVATIONS_CLASS,
                "label": GENERAL_OBSERVATIONS_CLASS,
                "classVariables": [
                    {
                        "name": "STUDYID",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 1,
                    },
                    {
                        "name": "DOMAIN",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 2,
                    },
                    {
                        "name": "USUBJID",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 3,
                    },
                    {
                        "name": "AETERM",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 4,
                    },
                ],
            },
        ],
    },
    {
        "_links": {"model": {"href": "/mdr/sdtm/1-5"}},
        "classes": [
            {
                "name": "Events",
                "datasets": [
                    {
                        "name": "AE",
                        "label": "Adverse Events",
                        "datasetVariables": [
                            {"name": "AETEST", "ordinal": 1},
                            {"name": "AENEW", "ordinal": 2},
                        ],
                    }
                ],
            }
        ],
    },
    {
        "STUDYID": ["TEST_STUDY", "TEST_STUDY", "TEST_STUDY"],
        "DOMAIN": ["AE", "AE", "AE"],
        "USUBJID": ["SUBJ001", "SUBJ002", "SUBJ003"],
        "AETERM": ["Headache", "Nausea", "Fatigue"],
    },
    {"name": "AE"},
    "role",
    "Identifier",
    ["STUDYID", "DOMAIN", "USUBJID", "AETERM"],
)

wildcard_test = (
    {
        "datasets": [
            {
                "_links": {"parentClass": {"title": "Events"}},
                "name": "AE",
                "datasetVariables": [
                    {"name": "AETERM", "ordinal": 4},
                    {
                        "name": "AESEQ",
                        "ordinal": 3,
                        "role": VariableRoles.IDENTIFIER.value,
                    },
                ],
            }
        ],
        "classes": [
            {
                "name": "Events",
                "label": "Events",
                "classVariables": [
                    {"name": "--TERM", "ordinal": 1},
                    {
                        "name": "--SEQ",
                        "ordinal": 2,
                        "role": VariableRoles.IDENTIFIER.value,
                    },
                ],
            },
            {
                "name": "FINDINGS",
                "label": "FINDINGS",
                "classVariables": [
                    {"name": "--TEST", "ordinal": 1},
                    {"name": "--ORRES", "ordinal": 2},
                ],
            },
            {
                "name": GENERAL_OBSERVATIONS_CLASS,
                "label": GENERAL_OBSERVATIONS_CLASS,
                "classVariables": [
                    {
                        "name": "STUDYID",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 1,
                    },
                    {
                        "name": "DOMAIN",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 2,
                    },
                ],
            },
        ],
    },
    {
        "_links": {"model": {"href": "/mdr/sdtm/1-5"}},
        "classes": [
            {
                "name": "Events",
                "datasets": [
                    {
                        "name": "AE",
                        "label": "Adverse Events",
                        "datasetVariables": [
                            {"name": "AETERM", "ordinal": 1},
                            {
                                "name": "AESEQ",
                                "ordinal": 2,
                                "role": VariableRoles.IDENTIFIER.value,
                            },
                        ],
                    }
                ],
            }
        ],
    },
    {
        "STUDYID": ["TEST_STUDY", "TEST_STUDY"],
        "AETERM": ["Headache", "Nausea"],
        "AESEQ": [1, 2],
    },
    {"name": "AE"},
    "role",
    "Identifier",
    ["STUDYID", "AESEQ"],
)

no_match_test = (
    {
        "datasets": [
            {
                "_links": {"parentClass": {"title": "Events"}},
                "name": "AE",
                "datasetVariables": [
                    {"name": "AETERM", "ordinal": 4},
                    {"name": "AESEQ", "ordinal": 3},
                ],
            }
        ],
        "classes": [
            {
                "name": "Events",
                "label": "Events",
                "classVariables": [
                    {"name": "--TERM", "ordinal": 1},
                    {
                        "name": "--SEQ",
                        "ordinal": 2,
                        "role": VariableRoles.IDENTIFIER.value,
                    },
                ],
            },
            {
                "name": GENERAL_OBSERVATIONS_CLASS,
                "label": GENERAL_OBSERVATIONS_CLASS,
                "classVariables": [
                    {
                        "name": "VISITNUM",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 17,
                    },
                    {
                        "name": "VISIT",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 18,
                    },
                ],
            },
        ],
    },
    {
        "_links": {"model": {"href": "/mdr/sdtm/1-5"}},
        "classes": [
            {
                "name": "Events",
                "datasets": [
                    {
                        "name": "AE",
                        "label": "Adverse Events",
                        "datasetVariables": [
                            {"name": "AETERM", "ordinal": 1},
                            {"name": "AESEQ", "ordinal": 2},
                        ],
                    }
                ],
            }
        ],
    },
    {
        "STUDYID": ["TEST_STUDY", "TEST_STUDY"],
        "AETERM": ["Headache", "Nausea"],
        "AESEQ": [1, 2],
    },
    {"name": "AE"},
    "role",
    "Timing",
    [],
)

findings_about_test = (
    {
        "datasets": [
            {
                "_links": {"parentClass": {"title": FINDINGS_ABOUT}},
                "name": "FA",
                "datasetVariables": [
                    {"name": "FAOBJ", "ordinal": 4},
                    {
                        "name": "FASEQ",
                        "ordinal": 3,
                        "role": VariableRoles.IDENTIFIER.value,
                    },
                ],
            }
        ],
        "classes": [
            {
                "name": FINDINGS_ABOUT,
                "label": FINDINGS_ABOUT,
                "classVariables": [
                    {"name": "--OBJ", "ordinal": 1},
                    {
                        "name": "USUBJID",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 2,
                    },
                    {
                        "name": "TIMING_VAR1",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 31,
                    },
                ],
            },
            {
                "name": "FINDINGS",
                "label": "FINDINGS",
                "classVariables": [
                    {"name": "--TEST", "ordinal": 1},
                    {"name": "--ORRES", "ordinal": 2},
                ],
            },
            {
                "name": GENERAL_OBSERVATIONS_CLASS,
                "label": GENERAL_OBSERVATIONS_CLASS,
                "classVariables": [
                    {
                        "name": "DOMAIN",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 2,
                    },
                    {
                        "name": "STUDYID",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 1,
                    },
                ],
            },
        ],
    },
    {
        "_links": {"model": {"href": "/mdr/sdtm/1-5"}},
        "classes": [
            {
                "name": FINDINGS_ABOUT,
                "datasets": [
                    {
                        "name": "FA",
                        "label": "Findings About",
                        "datasetVariables": [
                            {"name": "FAOBJ", "ordinal": 1},
                            {
                                "name": "FASEQ",
                                "ordinal": 2,
                                "role": VariableRoles.IDENTIFIER.value,
                            },
                        ],
                    }
                ],
            }
        ],
    },
    {
        "STUDYID": ["TEST_STUDY", "TEST_STUDY"],
        "DOMAIN": ["FA", "FA"],
        "FAOBJ": ["Object1", "Object2"],
        "FASEQ": [1, 2],
        "USUBJID": ["SUBJ001", "SUBJ002"],
    },
    {"name": "FA"},
    "role",
    "Identifier",
    ["STUDYID", "DOMAIN", "USUBJID"],
)


@pytest.mark.parametrize(
    "model_metadata, standard_metadata, study_data, dataset_metadata, key_name, key_value, expected_variables",
    [
        events_timing_test,
        events_identifier_test,
        wildcard_test,
        no_match_test,
        findings_about_test,
    ],
)
def test_get_dataset_filtered_variables(
    operation_params: OperationParams,
    model_metadata: dict,
    standard_metadata: dict,
    study_data: dict,
    dataset_metadata: dict,
    key_name: str,
    key_value: str,
    expected_variables: List[str],
):
    operation_params.dataframe = PandasDataset.from_dict(study_data)
    operation_params.domain = dataset_metadata["name"]
    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"
    operation_params.key_name = key_name
    operation_params.key_value = key_value
    operation_params.datasets = [SDTMDatasetMetadata(**dataset_metadata)]

    cache = InMemoryCacheService.get_instance()
    library_metadata = LibraryMetadataContainer(
        standard_metadata=standard_metadata, model_metadata=model_metadata
    )

    data_service = LocalDataService(
        cache_service=cache,
        config=ConfigService(),
        reader_factory=DataReaderFactory(),
        standard="sdtmig",
        standard_version="3-4",
        library_metadata=library_metadata,
    )

    expected_class = (
        EVENTS
        if model_metadata["datasets"][0]["_links"]["parentClass"]["title"] == "Events"
        else FINDINGS_ABOUT
    )

    def mock_get_raw_metadata(*args, **kwargs):
        return SDTMDatasetMetadata(**dataset_metadata)

    data_service.get_raw_dataset_metadata = mock_get_raw_metadata

    with patch.object(
        LocalDataService, "get_dataset_class", return_value=expected_class
    ):
        operation = GetDatasetFilteredVariables(
            operation_params,
            operation_params.dataframe,
            cache,
            data_service,
            library_metadata,
        )

    result = operation.execute()

    assert operation_params.operation_id in result
    expected = pd.Series(
        [expected_variables] * len(study_data[list(study_data.keys())[0]])
    )
    assert result[operation_params.operation_id].equals(expected)


def test_get_dataset_filtered_variables_dask(
    operation_params: OperationParams,
):
    """Test GetDatasetFilteredVariables operation with DaskDataset"""
    study_data = {
        "STUDYID": ["TEST_STUDY", "TEST_STUDY", "TEST_STUDY"],
        "DOMAIN": ["AE", "AE", "AE"],
        "USUBJID": ["SUBJ001", "SUBJ002", "SUBJ003"],
        "AETERM": ["Headache", "Nausea", "Fatigue"],
        "VISITNUM": [1, 2, 1],
        "VISIT": ["Day 1", "Day 7", "Day 1"],
    }

    operation_params.dataframe = DaskDataset.from_dict(study_data)
    operation_params.domain = "AE"
    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"
    operation_params.key_name = "role"
    operation_params.key_value = "Timing"
    operation_params.datasets = [SDTMDatasetMetadata(name="AE")]

    model_metadata = {
        "datasets": [
            {
                "_links": {"parentClass": {"title": "Events"}},
                "name": "AE",
                "datasetVariables": [
                    {"name": "USUBJID", "ordinal": 2},
                    {"name": "AESEQ", "ordinal": 3},
                    {"name": "AETERM", "ordinal": 4},
                    {
                        "name": "VISITNUM",
                        "ordinal": 17,
                        "role": VariableRoles.TIMING.value,
                    },
                    {
                        "name": "VISIT",
                        "ordinal": 18,
                        "role": VariableRoles.TIMING.value,
                    },
                ],
            }
        ],
        "classes": [
            {
                "name": "Events",
                "label": "Events",
                "classVariables": [
                    {"name": "--TERM", "ordinal": 1},
                    {"name": "--SEQ", "ordinal": 2},
                ],
            },
            {
                "name": GENERAL_OBSERVATIONS_CLASS,
                "label": GENERAL_OBSERVATIONS_CLASS,
                "classVariables": [
                    {
                        "name": "STUDYID",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 1,
                    },
                    {
                        "name": "DOMAIN",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 2,
                    },
                    {
                        "name": "USUBJID",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 3,
                    },
                    {
                        "name": "VISITNUM",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 17,
                    },
                    {
                        "name": "VISIT",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 18,
                    },
                    {
                        "name": "AETIMING",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 33,
                    },
                ],
            },
        ],
    }

    standard_metadata = {
        "_links": {"model": {"href": "/mdr/sdtm/1-5"}},
        "classes": [
            {
                "name": "Events",
                "datasets": [
                    {
                        "name": "AE",
                        "label": "Adverse Events",
                        "datasetVariables": [
                            {"name": "AETEST", "ordinal": 1},
                            {"name": "AENEW", "ordinal": 2},
                            {
                                "name": "VISITNUM",
                                "ordinal": 3,
                                "role": VariableRoles.TIMING.value,
                            },
                            {
                                "name": "VISIT",
                                "ordinal": 4,
                                "role": VariableRoles.TIMING.value,
                            },
                        ],
                    }
                ],
            }
        ],
    }

    cache = InMemoryCacheService.get_instance()
    library_metadata = LibraryMetadataContainer(
        standard_metadata=standard_metadata, model_metadata=model_metadata
    )

    data_service = LocalDataService(
        cache_service=cache,
        config=ConfigService(),
        reader_factory=DataReaderFactory(),
        standard="sdtmig",
        standard_version="3-4",
        library_metadata=library_metadata,
    )

    def mock_get_raw_metadata(*args, **kwargs):
        return SDTMDatasetMetadata(name="AE")

    data_service.get_raw_dataset_metadata = mock_get_raw_metadata

    with patch.object(LocalDataService, "get_dataset_class", return_value=EVENTS):
        operation = GetDatasetFilteredVariables(
            operation_params,
            operation_params.dataframe,
            cache,
            data_service,
            library_metadata,
        )

    result = operation.execute()

    assert operation_params.operation_id in result
    expected_variables = ["VISITNUM", "VISIT"]
    expected = pd.Series(
        [expected_variables] * len(study_data[list(study_data.keys())[0]])
    )
    assert result[operation_params.operation_id].equals(expected)


def test_get_dataset_filtered_variables_empty_dataset(
    operation_params: OperationParams,
):
    operation_params.dataframe = PandasDataset.from_dict({})
    operation_params.domain = "AE"
    operation_params.key_name = "role"
    operation_params.key_value = "Timing"
    operation_params.datasets = [SDTMDatasetMetadata(name="AE")]

    model_metadata = {
        "datasets": [
            {
                "_links": {"parentClass": {"title": "Events"}},
                "name": "AE",
                "datasetVariables": [],
            }
        ],
        "classes": [
            {
                "name": "Events",
                "classVariables": [
                    {
                        "name": "VISITNUM",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 1,
                    },
                ],
            },
            {
                "name": GENERAL_OBSERVATIONS_CLASS,
                "label": GENERAL_OBSERVATIONS_CLASS,
                "classVariables": [
                    {
                        "name": "VISITNUM",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 17,
                    },
                    {
                        "name": "VISIT",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 18,
                    },
                ],
            },
        ],
    }

    standard_metadata = {
        "_links": {"model": {"href": "/mdr/sdtm/1-5"}},
        "classes": [
            {
                "name": "Events",
                "datasets": [
                    {
                        "name": "AE",
                        "datasetVariables": [
                            {"name": "VISITNUM", "role": VariableRoles.TIMING.value},
                        ],
                    }
                ],
            }
        ],
    }

    cache = InMemoryCacheService.get_instance()
    library_metadata = LibraryMetadataContainer(
        standard_metadata=standard_metadata, model_metadata=model_metadata
    )

    data_service = LocalDataService(
        cache_service=cache,
        config=ConfigService(),
        reader_factory=DataReaderFactory(),
        standard="sdtmig",
        standard_version="3-4",
        library_metadata=library_metadata,
    )

    def mock_get_raw_metadata(*args, **kwargs):
        return SDTMDatasetMetadata(name="AE")

    data_service.get_raw_dataset_metadata = mock_get_raw_metadata

    with patch.object(LocalDataService, "get_dataset_class", return_value=EVENTS):
        operation = GetDatasetFilteredVariables(
            operation_params,
            operation_params.dataframe,
            cache,
            data_service,
            library_metadata,
        )

    result = operation.execute()

    assert operation_params.operation_id in result
    assert len(result[operation_params.operation_id]) == 0


def test_get_dataset_filtered_variables_invalid_key(operation_params: OperationParams):
    operation_params.dataframe = PandasDataset.from_dict(
        {
            "STUDYID": ["TEST"],
            "AETERM": ["Headache"],
        }
    )
    operation_params.domain = "AE"
    operation_params.key_name = "invalid_key"
    operation_params.key_value = "SomeValue"
    operation_params.datasets = [SDTMDatasetMetadata(name="AE")]

    model_metadata = {
        "datasets": [
            {
                "_links": {"parentClass": {"title": "Events"}},
                "name": "AE",
                "datasetVariables": [
                    {
                        "name": "AETERM",
                        "ordinal": 1,
                        "role": VariableRoles.IDENTIFIER.value,
                    },
                ],
            }
        ],
        "classes": [
            {
                "name": "Events",
                "classVariables": [
                    {
                        "name": "--TERM",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 1,
                    },
                ],
            },
            {
                "name": GENERAL_OBSERVATIONS_CLASS,
                "label": GENERAL_OBSERVATIONS_CLASS,
                "classVariables": [
                    {
                        "name": "STUDYID",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 1,
                    },
                    {
                        "name": "DOMAIN",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 2,
                    },
                ],
            },
        ],
    }

    standard_metadata = {
        "_links": {"model": {"href": "/mdr/sdtm/1-5"}},
        "classes": [
            {
                "name": "Events",
                "datasets": [
                    {
                        "name": "AE",
                        "datasetVariables": [
                            {"name": "AETERM", "role": VariableRoles.IDENTIFIER.value},
                        ],
                    }
                ],
            }
        ],
    }

    cache = InMemoryCacheService.get_instance()
    library_metadata = LibraryMetadataContainer(
        standard_metadata=standard_metadata, model_metadata=model_metadata
    )

    data_service = LocalDataService(
        cache_service=cache,
        config=ConfigService(),
        reader_factory=DataReaderFactory(),
        standard="sdtmig",
        standard_version="3-4",
        library_metadata=library_metadata,
    )

    def mock_get_raw_metadata(*args, **kwargs):
        return SDTMDatasetMetadata(name="AE")

    data_service.get_raw_dataset_metadata = mock_get_raw_metadata

    with patch.object(LocalDataService, "get_dataset_class", return_value=EVENTS):
        operation = GetDatasetFilteredVariables(
            operation_params,
            operation_params.dataframe,
            cache,
            data_service,
            library_metadata,
        )

    result = operation.execute()

    assert operation_params.operation_id in result
    expected = pd.Series([[]])
    assert result[operation_params.operation_id].equals(expected)
