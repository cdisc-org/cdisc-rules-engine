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


def test_get_label_referenced_variable_metadata(operation_params: OperationParams):
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
    operation_params.dataframe = pd.DataFrame.from_dict(
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
    # execute operation
    data_service = LocalDataService.get_instance(
        cache_service=cache, config=ConfigService()
    )
    operation = LabelReferencedVariableMetadata(
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )
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
    assert result["$label_referenced_variable_name"][0] == "AETEST"
    assert result["$label_referenced_variable_name"][1] == "AENEW"

    # Check unmatched label in the AELABEL column
    assert result["$label_referenced_variable_name"][2] == ""
