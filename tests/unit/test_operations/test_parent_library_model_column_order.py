from typing import List
from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)

import pandas as pd
import pytest
from unittest.mock import patch

from cdisc_rules_engine.constants.classes import (
    GENERAL_OBSERVATIONS_CLASS,
    FINDINGS,
    FINDINGS_ABOUT,
    INTERVENTIONS,
)
from cdisc_rules_engine.enums.variable_roles import VariableRoles
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.parent_library_model_column_order import (
    ParentLibraryModelColumnOrder,
)
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.services.data_readers import DataReaderFactory


@pytest.mark.parametrize(
    "data, model_metadata, standard_metadata",
    [
        (
            PandasDataset.from_dict(
                {
                    "RDOMAIN": ["AE", "AE", "AE", "AE"],
                    "IDVAR": ["AESEQ", "AESEQ", "AESEQ", "AESEQ"],
                    "IDVARVAL": [1, 2, 1, 3],
                }
            ),
            {
                "datasets": [
                    {
                        "_links": {"parentClass": {"title": "Events"}},
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
                                    {"name": "AETEST", "ordinal": 1},
                                    {"name": "AENEW", "ordinal": 2},
                                ],
                            }
                        ],
                    }
                ],
            },
        ),
    ],
)
def test_get_parent_column_order_from_library(
    data: dict,
    operation_params: OperationParams,
    model_metadata: dict,
    standard_metadata: dict,
):
    datasets: List[dict] = [
        {
            "domain": "AE",
            "filename": "ae.xpt",
        }
    ]
    ae = PandasDataset.from_dict(
        {
            "AESTDY": [4, 5, 6],
            "STUDYID": [101, 201, 300],
            "AESEQ": [1, 2, 3],
        }
    )
    path_to_dataset_map: dict = {"ae.xpt": ae}
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        side_effect=lambda dataset_name: path_to_dataset_map[dataset_name],
    ):
        operation_params.dataframe = data
        operation_params.domain = "SUPPAE"
        operation_params.standard = "sdtmig"
        operation_params.standard_version = "3-4"
        operation_params.datasets = datasets

        # save model metadata to cache
        cache = InMemoryCacheService.get_instance()
        library_metadata = LibraryMetadataContainer(
            standard_metadata=standard_metadata, model_metadata=model_metadata
        )
        # execute operation
        data_service = LocalDataService.get_instance(
            cache_service=cache, config=ConfigService()
        )
        operation = ParentLibraryModelColumnOrder(
            operation_params,
            operation_params.dataframe,
            cache,
            data_service,
            library_metadata,
        )
        result: pd.DataFrame = operation.execute()
        variables: List[str] = [
            "STUDYID",
            "DOMAIN",
            "AETERM",
            "AESEQ",
            "TIMING_VAR",
        ]
        expected: pd.Series = pd.Series(
            [
                variables,
                variables,
                variables,
                variables,
            ]
        )
        assert result[operation_params.operation_id].equals(expected)


@pytest.mark.parametrize(
    "data, model_metadata, standard_metadata",
    [
        (
            DaskDataset.from_dict(
                {
                    "RDOMAIN": ["AE", "AE", "AE", "AE"],
                    "IDVAR": ["AESEQ", "AESEQ", "AESEQ", "AESEQ"],
                    "IDVARVAL": [1, 2, 1, 3],
                }
            ),
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
                        "name": INTERVENTIONS,
                        "label": INTERVENTIONS,
                        "classVariables": [
                            {"name": "--VAR1", "ordinal": 1},
                            {"name": "--TRT", "ordinal": 2},
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
        )
    ],
)
def test_get_parent_findings_class_column_order_from_library(
    data: dict,
    operation_params: OperationParams,
    model_metadata: dict,
    standard_metadata: dict,
):
    datasets: List[dict] = [
        {
            "domain": "AE",
            "filename": "ae.xpt",
        },
        {
            "domain": "EC",
            "filename": "ec.xpt",
        },
    ]
    ae = DaskDataset.from_dict(
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
        }
    )
    ec = DaskDataset.from_dict(
        {
            "ECSTDY": [500, 4],
            "STUDYID": [201, 101],
            "DOMAIN": ["EC", "EC"],
            "ECSEQ": [2, 1],
            "ECTRT": [2, 1],
        }
    )
    path_to_dataset_map: dict = {
        "ae.xpt": ae,
        "ec.xpt": ec,
    }
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        side_effect=lambda dataset_name: path_to_dataset_map[dataset_name],
    ):
        operation_params.dataframe = data
        operation_params.domain = "SUPPAE"
        operation_params.standard = "sdtmig"
        operation_params.standard_version = "3-4"
        operation_params.datasets = datasets

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
        operation = ParentLibraryModelColumnOrder(
            operation_params,
            operation_params.dataframe,
            cache,
            data_service,
            library_metadata,
        )
        result: pd.DataFrame = operation.execute()
        variables: List[str] = [
            "STUDYID",
            "DOMAIN",
            "AEVAR1",
            "AETEST",
            "AEOBJ",
            "AEVAR2",
            "TIMING_VAR",
        ]
        expected: pd.Series = pd.Series(
            [
                variables,
                variables,
                variables,
                variables,
            ]
        )
        assert result[operation_params.operation_id].equals(expected)
