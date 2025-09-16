from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
import pandas as pd
from cdisc_rules_engine.constants.classes import GENERAL_OBSERVATIONS_CLASS
from cdisc_rules_engine.enums.variable_roles import VariableRoles
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.label_referenced_variable_metadata import (
    LabelReferencedVariableMetadata,
)
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
import pytest
from unittest.mock import Mock, patch


@pytest.mark.parametrize("dataset_type", [(PandasDataset)])
def test_get_label_referenced_variable_metadata(
    operation_params: OperationParams, dataset_type
):
    model_metadata = {
        "datasets": [
            {
                "name": "AE",
                "datasetVariables": [
                    {
                        "name": "AETERM",
                        "ordinal": 4,
                    },
                    {
                        "name": "AESEQ",
                        "ordinal": 3,
                    },
                ],
            }
        ],
        "classes": [
            {
                "name": "Events",
                "classVariables": [
                    {"name": "--TERM", "ordinal": 1},
                    {"name": "--SEQ", "ordinal": 2},
                ],
            },
            {
                "name": GENERAL_OBSERVATIONS_CLASS,
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
                    {
                        "name": "TIMING_VAR",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 33,
                    },
                ],
            },
        ],
    }
    standard_metadata = {
        "_links": {"model": {"href": "/mdr/sdtm/1-5"}},
        "domains": {
            "HO",
            "CO",
            "SU",
            "PP",
            "TM",
            "TD",
            "SS",
            "TR",
            "CV",
            "EX",
            "RELSPEC",
            "FA",
            "SR",
            "SV",
            "TI",
            "CM",
            "RE",
            "TU",
            "ML",
            "RELSUB",
            "SUPPQUAL",
            "TA",
            "UR",
            "RS",
            "VS",
            "EC",
            "IS",
            "DV",
            "RELREC",
            "PR",
            "SM",
            "EG",
            "MK",
            "TS",
            "DS",
            "PE",
            "DM",
            "MH",
            "GF",
            "BE",
            "OE",
            "CE",
            "CP",
            "MS",
            "DD",
            "TV",
            "MI",
            "FT",
            "PC",
            "RP",
            "IE",
            "TE",
            "LB",
            "BS",
            "QS",
            "SC",
            "AG",
            "DA",
            "SE",
            "AE",
            "OI",
            "MB",
            "NV",
        },
        "classes": [
            {
                "name": "Events",
                "datasets": [
                    {
                        "name": "AE",
                        "label": "Adverse Events",
                        "datasetVariables": [
                            {"name": "AETEST", "ordinal": 1, "label": "TEST AE"},
                            {"name": "AENEW", "ordinal": 2, "label": "NEW AE"},
                        ],
                    }
                ],
            }
        ],
    }
    operation_params.dataframe = dataset_type.from_dict(
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
            "AELABEL": ["TEST AE", "NEW AE", "TEST A"],
        }
    )
    operation_params.domain = "AE"
    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"
    operation_params.target = "AELABEL"
    operation_params.operation_id = "$label_referenced_variable"
    # save model metadata to cache
    cache = InMemoryCacheService.get_instance()

    library_metadata = LibraryMetadataContainer(
        standard_metadata=standard_metadata, model_metadata=model_metadata
    )
    mock_dataset_class = Mock()
    mock_dataset_class.name = "Events"
    # execute operation
    data_service = LocalDataService.get_instance(
        cache_service=cache, config=ConfigService()
    )
    data_service.get_dataset_class = Mock(return_value=mock_dataset_class)
    operation = LabelReferencedVariableMetadata(
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )

    def mock_cached_method(*args, **kwargs):
        return operation_params.dataframe

    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_raw_dataset_metadata",
        side_effect=mock_cached_method,
    ):
        result: pd.DataFrame = operation.execute()
    expected_columns = [
        "STUDYID",
        "AETERM",
        "AELABEL",
        "$label_referenced_variable_name",
        "$label_referenced_variable_role",
        "$label_referenced_variable_ordinal",
        "$label_referenced_variable_label",
    ]

    assert result.columns.to_list() == expected_columns
    assert (
        result.data[result["$label_referenced_variable_label"] == "TEST AE"][
            "$label_referenced_variable_name"
        ].values
        == "AETEST"
    )
    assert (
        result.data[result["$label_referenced_variable_label"] == "NEW AE"][
            "$label_referenced_variable_name"
        ].values
        == "AENEW"
    )


@pytest.mark.parametrize("dataset_type", [(PandasDataset)])
def test_get_label_referenced_variable_metadata_missing_role_field(
    operation_params: OperationParams, dataset_type
):
    """Test that the operation works correctly when no variables have a role field."""
    model_metadata = {
        "datasets": [
            {
                "name": "FA",
                "datasetVariables": [
                    {
                        "name": "FATERM",
                        "ordinal": 4,
                    },  # No role field
                ],
            }
        ],
        "classes": [
            {
                "name": "FINDINGS ABOUT",
                "classVariables": [
                    {"name": "--TERM", "ordinal": 1},  # No role field
                ],
            },
            {
                "name": "FINDINGS",
                "classVariables": [
                    {"name": "--TEST", "ordinal": 1},  # No role field
                    {"name": "--TESTCD", "ordinal": 2},  # No role field
                ],
            },
            {
                "name": GENERAL_OBSERVATIONS_CLASS,
                "classVariables": [
                    {
                        "name": "DOMAIN",
                        "ordinal": 2,
                    },  # No role field
                    {
                        "name": "STUDYID",
                        "ordinal": 1,
                    },  # No role field
                ],
            },
        ],
    }
    standard_metadata = {
        "_links": {"model": {"href": "/mdr/sdtm/1-5"}},
        "domains": {"FA"},
        "classes": [
            {
                "name": "FINDINGS ABOUT",
                "datasets": [
                    {
                        "name": "FA",
                        "label": "Findings About",
                        "datasetVariables": [
                            {"name": "FATESTCD", "ordinal": 1, "label": "Test Code"},
                            {"name": "FATEST", "ordinal": 2, "label": "Test Name"},
                        ],
                    }
                ],
            }
        ],
    }
    operation_params.dataframe = dataset_type.from_dict(
        {
            "STUDYID": ["TEST_STUDY", "TEST_STUDY"],
            "DOMAIN": ["FA", "FA"],
            "FATERM": ["Test Code", "Test Name"],
        }
    )
    operation_params.domain = "FA"
    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"
    operation_params.target = "FATERM"
    operation_params.operation_id = "$label_referenced_variable"

    # save model metadata to cache
    cache = InMemoryCacheService.get_instance()

    library_metadata = LibraryMetadataContainer(
        standard_metadata=standard_metadata, model_metadata=model_metadata
    )
    mock_dataset_class = Mock()
    mock_dataset_class.name = "Findings About"
    # execute operation
    data_service = LocalDataService.get_instance(
        cache_service=cache, config=ConfigService()
    )
    data_service.get_dataset_class = Mock(return_value=mock_dataset_class)
    operation = LabelReferencedVariableMetadata(
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )

    def mock_cached_method(*args, **kwargs):
        return operation_params.dataframe

    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_raw_dataset_metadata",
        side_effect=mock_cached_method,
    ):
        result: pd.DataFrame = operation.execute()

    # Test that all expected columns are present, even when no variables have a role field
    expected_columns = [
        "STUDYID",
        "DOMAIN",
        "FATERM",
        "$label_referenced_variable_name",
        "$label_referenced_variable_role",
        "$label_referenced_variable_ordinal",
        "$label_referenced_variable_label",
    ]

    actual_columns = result.columns.to_list()

    # Check that all expected columns are present (order doesn't matter)
    for col in expected_columns:
        assert col in actual_columns, f"Expected column {col} not found in result"

    # Test that the role column exists and can be accessed (even if empty)
    role_values = result.data["$label_referenced_variable_role"].tolist()
    assert isinstance(role_values, list)
    assert len(role_values) == 2  # Should have 2 entries for the 2 test rows

    # Test that the operation correctly matched the labels to variable names
    assert (
        result.data[result["$label_referenced_variable_label"] == "Test Code"][
            "$label_referenced_variable_name"
        ].values
        == "FATESTCD"
    )
    assert (
        result.data[result["$label_referenced_variable_label"] == "Test Name"][
            "$label_referenced_variable_name"
        ].values
        == "FATEST"
    )
