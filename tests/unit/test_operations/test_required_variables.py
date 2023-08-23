from typing import List
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)

import pandas as pd
import pytest

from cdisc_rules_engine.constants.classes import GENERAL_OBSERVATIONS_CLASS
from cdisc_rules_engine.enums.variable_roles import VariableRoles
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.required_variables import RequiredVariables
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService


@pytest.mark.parametrize(
    "model_metadata, standard_metadata",
    [
        (
            {
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
                                "name": "--SEQ",
                                "role": VariableRoles.IDENTIFIER.value,
                                "ordinal": 12,
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
                                    {"name": "AETEST", "ordinal": 1, "core": "Req"},
                                    {"name": "AENEW", "ordinal": 2, "core": "Exp"},
                                ],
                            }
                        ],
                    }
                ],
            },
        )
    ],
)
def test_get_required_variables(
    operation_params: OperationParams, model_metadata: dict, standard_metadata: dict
):
    """
    Unit test for DataProcessor.get_column_order_from_library.
    Mocks cache call to return metadata.
    """
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
        }
    )
    operation_params.domain = "AE"
    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"

    # save model metadata to cache
    cache = InMemoryCacheService.get_instance()
    library_metadata = LibraryMetadataContainer(
        standard_metadata=standard_metadata, model_metadata=model_metadata
    )
    # execute operation
    data_service = LocalDataService.get_instance(cache_service=cache)
    operation = RequiredVariables(
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )
    result: pd.DataFrame = operation.execute()
    variables: List[str] = sorted(["STUDYID", "DOMAIN", "AESEQ", "AETEST"])
    for result_array in result[operation_params.operation_id]:
        assert sorted(result_array) == variables
