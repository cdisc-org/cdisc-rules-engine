from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
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
from cdisc_rules_engine.operations.get_model_filtered_variables import (
    LibraryModelVariablesFilter,
)
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.config import ConfigService
from cdisc_rules_engine.services.data_readers import DataReaderFactory
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata

test_set1 = (
    {
        "datasets": [
            {
                "_links": {"parentClass": {"title": "Events"}},
                "name": "AE",
                "datasetVariables": [
                    {
                        "name": "USUBJID",
                        "ordinal": 2,
                    },
                    {
                        "name": "AESEQ",
                        "ordinal": 3,
                    },
                    {
                        "name": "AETERM",
                        "ordinal": 4,
                    },
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
                        "name": "AETERM",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 4,
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
                        "name": "TIMING_VAR",
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
        "STUDYID": [
            "TEST_STUDY",
            "TEST_STUDY",
            "TEST_STUDY",
        ],
        "AETERM": [
            "test",
            "test",
            "test",
        ],
    },
    {"name": "AE"},
    "Timing",
    ["VISITNUM", "VISIT", "TIMING_VAR"],
)

test_set2 = (
    {
        "datasets": [
            {
                "_links": {"parentClass": {"title": "Events"}},
                "name": "AE",
                "datasetVariables": [
                    {
                        "name": "USUBJID",
                        "ordinal": 2,
                    },
                    {
                        "name": "AESEQ",
                        "ordinal": 3,
                    },
                    {
                        "name": "AETERM",
                        "ordinal": 4,
                    },
                    {
                        "name": "VISITNUM",
                        "ordinal": 17,
                    },
                    {
                        "name": "VISIT",
                        "ordinal": 18,
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
                        "name": "AETERM",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 4,
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
                        "name": "TIMING_VAR",
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
                            {"name": "VISITNUM", "ordinal": 3},
                            {"name": "VISIT", "ordinal": 4},
                        ],
                    }
                ],
            }
        ],
    },
    {
        "STUDYID": [
            "TEST_STUDY",
            "TEST_STUDY",
            "TEST_STUDY",
        ],
        "AETERM": [
            "test",
            "test",
            "test",
        ],
    },
    {"name": "AE"},
    "Identifier",
    ["STUDYID", "DOMAIN", "USUBJID", "AETERM"],
)


@pytest.mark.parametrize(
    "model_metadata, standard_metadata, study_data, dataset_metadata, key_val, var_list",
    [test_set1, test_set2],
)
def test_get_model_filtered_variables(
    operation_params: OperationParams,
    model_metadata: dict,
    standard_metadata: dict,
    study_data: dict,
    dataset_metadata: dict,
    key_val: str,
    var_list: List[str],
):
    if key_val is None:
        key_val = "Timing"
    operation_params.dataframe = PandasDataset.from_dict(study_data)
    operation_params.domain = "AE"
    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"
    operation_params.key_name = "role"
    operation_params.key_value = key_val
    operation_params.datasets = [SDTMDatasetMetadata(**dataset_metadata)]
    # save model metadata to cache
    cache = InMemoryCacheService.get_instance()
    library_metadata = LibraryMetadataContainer(
        standard_metadata=standard_metadata, model_metadata=model_metadata
    )
    # execute operation
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
    """
    this fuction replaces get_raw_dataset_metadata in LocalDataService to
    prevent filtering into the decorator that checks cache
    """

    def mock_get_raw_metadata(*args, **kwargs):
        return SDTMDatasetMetadata(**dataset_metadata)

    data_service.get_raw_dataset_metadata = mock_get_raw_metadata
    with patch.object(
        LocalDataService, "get_dataset_class", return_value=expected_class
    ):
        operation = LibraryModelVariablesFilter(
            operation_params,
            operation_params.dataframe,
            cache,
            data_service,
            library_metadata,
        )
    operation = LibraryModelVariablesFilter(
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )
    result = operation.execute()
    variables: List[str] = var_list
    expected = pd.Series(
        [
            variables,
            variables,
            variables,
        ]
    )
    assert result[operation_params.operation_id].equals(expected)
