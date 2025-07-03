from typing import List
from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)

import pandas as pd
import pytest
from unittest.mock import Mock, patch
from cdisc_rules_engine.constants.classes import GENERAL_OBSERVATIONS_CLASS
from cdisc_rules_engine.enums.variable_roles import VariableRoles
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.library_column_order import LibraryColumnOrder
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService


@pytest.mark.parametrize(
    "model_metadata, standard_metadata, dataset_type",
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
                                    {"name": "AETEST", "ordinal": 4},
                                    {"name": "AENEW", "ordinal": 5},
                                ],
                            }
                        ],
                    }
                ],
            },
            PandasDataset,
        ),
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
                                    {"name": "AETEST", "ordinal": 4},
                                    {"name": "AENEW", "ordinal": 5},
                                ],
                            }
                        ],
                    }
                ],
            },
            DaskDataset,
        ),
    ],
)
def test_get_column_order_from_library(
    operation_params: OperationParams,
    model_metadata: dict,
    standard_metadata: dict,
    dataset_type,
):
    """
    Unit test for DataProcessor.get_column_order_from_library.
    Mocks cache call to return metadata.
    """
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
    mock_dataset_class = Mock()
    mock_dataset_class.name = "Events"
    # execute operation
    data_service = LocalDataService.get_instance(
        cache_service=cache, config=ConfigService()
    )
    data_service.get_dataset_class = Mock(return_value=mock_dataset_class)
    operation = LibraryColumnOrder(
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
        result = operation.execute()
    variables: List[str] = [
        "STUDYID",
        "DOMAIN",
        "AETERM",
        "AESEQ",
        "AETEST",
        "AENEW",
        "TIMING_VAR",
    ]
    expected: pd.Series = pd.Series(
        [
            variables,
            variables,
            variables,
        ]
    )
    assert result[operation_params.operation_id].equals(expected)
