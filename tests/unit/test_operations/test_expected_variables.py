from typing import List
from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)

import pandas as pd
import dask.dataframe as dd

import pytest

from cdisc_rules_engine.constants.classes import GENERAL_OBSERVATIONS_CLASS
from cdisc_rules_engine.enums.variable_roles import VariableRoles
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.DatasetOperations.Operations import DatasetOperations


@pytest.mark.parametrize(
    "dataframe, model_metadata, standard_metadata",
    [
        (
            pd.DataFrame.from_dict(
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
            ),
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
        ),
        (
            dd.from_pandas(
                pd.DataFrame.from_dict(
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
                ),
                npartitions=1,
            ),
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
        ),
    ],
)
def test_get_expected_variables(
    operation_params: OperationParams,
    dataframe,
    model_metadata: dict,
    standard_metadata: dict,
):
    """
    Unit test for DataProcessor.get_column_order_from_library.
    Mocks cache call to return metadata.
    """
    operation_params.dataframe = dataframe
    operation_params.domain = "AE"
    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"

    # save model metadata to cache
    cache = InMemoryCacheService.get_instance()
    library_metadata = LibraryMetadataContainer(
        standard_metadata=standard_metadata, model_metadata=model_metadata
    )
    # execute operation
    data_service = LocalDataService.get_instance(
        cache_service=cache, config=ConfigService()
    )
    operations = DatasetOperations()
    result = operations.get_service(
        "expected_variables",
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )
    variables: List[str] = ["AENEW"]
    expected: pd.Series = pd.Series(
        [
            variables,
            variables,
            variables,
        ]
    )
    for i in range(len(expected)):
        assert result[operation_params.operation_id].iloc[i] == expected[i]
