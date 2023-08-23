# python -m pytest -s tests/unit/test_operations/test_get_model_filtered_variables.py

from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
import pandas as pd
import pytest

from typing import List

from cdisc_rules_engine.constants.classes import (
    GENERAL_OBSERVATIONS_CLASS,
    FINDINGS,
    FINDINGS_ABOUT,
)
from cdisc_rules_engine.enums.variable_roles import VariableRoles
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.get_model_filtered_variables import (
    LibraryModelVariablesFilter,
)
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.config import ConfigService

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
    "Identifier",
    ["STUDYID", "DOMAIN", "USUBJID", "AETERM"],
)

test_set3 = (
    {
        "datasets": [
            {
                "_links": {"parentClass": {"title": FINDINGS_ABOUT}},
                "name": "NOTTHESAME",
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
                        "name": "IDVAR1",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 3,
                    },
                    {
                        "name": "TIMING_VAR1",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 31,
                    },
                    {
                        "name": "TIMING_VAR2",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 32,
                    },
                ],
            },
            {
                "name": FINDINGS,
                "label": FINDINGS,
                "classVariables": [
                    {"name": "--VAR1", "ordinal": 1},
                    {"name": "--TEST", "ordinal": 2},
                    {"name": "--VAR2", "ordinal": 3},
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
                    {
                        "name": "TIMING_VAR1",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 32,
                    },
                    {
                        "name": "TIMING_VAR2",
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
                "name": FINDINGS_ABOUT,
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
        "STUDYID": [
            "TEST_STUDY",
            "TEST_STUDY",
            "TEST_STUDY",
        ],
        "DOMAIN": ["AE", "AE", "AE"],
        "AEOBJ": [
            "test",
            "test",
            "test",
        ],
        "AETESTCD": ["test", "test", "test"],
    },
    "Timing",
    ["TIMING_VAR1", "TIMING_VAR2"],
)

test_set4 = (
    {
        "datasets": [
            {
                "_links": {"parentClass": {"title": FINDINGS_ABOUT}},
                "name": "NOTTHESAME",
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
                        "name": "IDVAR1",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 3,
                    },
                    {
                        "name": "TIMING_VAR1",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 31,
                    },
                    {
                        "name": "TIMING_VAR2",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 32,
                    },
                ],
            },
            {
                "name": FINDINGS,
                "label": FINDINGS,
                "classVariables": [
                    {"name": "--VAR1", "ordinal": 1},
                    {"name": "--TEST", "ordinal": 2},
                    {"name": "--VAR2", "ordinal": 3},
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
                    {
                        "name": "IDVAR1",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 3,
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
                "name": FINDINGS_ABOUT,
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
        "STUDYID": [
            "TEST_STUDY",
            "TEST_STUDY",
            "TEST_STUDY",
        ],
        "DOMAIN": ["AE", "AE", "AE"],
        "AEOBJ": [
            "test",
            "test",
            "test",
        ],
        "AETESTCD": ["test", "test", "test"],
    },
    "Identifier",
    ["STUDYID", "DOMAIN", "IDVAR1"],
)


@pytest.mark.parametrize(
    "model_metadata, standard_metadata, study_data, key_val, var_list",
    [test_set1, test_set2, test_set3, test_set4],
)
def test_get_model_filtered_variables(
    operation_params: OperationParams,
    model_metadata: dict,
    standard_metadata: dict,
    study_data: dict,
    key_val: str,
    var_list: List[str],
):
    """
    Unit test for DataProcessor.get_column_order_from_library.
    Mocks cache call to return metadata.
    """
    if key_val is None:
        key_val = "Timing"
    operation_params.dataframe = pd.DataFrame.from_dict(study_data)
    operation_params.domain = "AE"
    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"
    operation_params.key_name = "role"
    operation_params.key_value = key_val

    # save model metadata to cache
    cache = InMemoryCacheService.get_instance()
    library_metadata = LibraryMetadataContainer(
        standard_metadata=standard_metadata, model_metadata=model_metadata
    )
    # execute operation
    data_service = LocalDataService.get_instance(
        cache_service=cache, config=ConfigService()
    )

    operation = LibraryModelVariablesFilter(
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )

    result: pd.DataFrame = operation.execute()

    variables: List[str] = var_list
    expected: pd.Series = pd.Series(
        [
            variables,
            variables,
            variables,
        ]
    )
    assert result[operation_params.operation_id].equals(expected)
