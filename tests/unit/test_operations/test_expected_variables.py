from typing import List
from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
import pandas as pd
import pytest

from cdisc_rules_engine.constants.classes import GENERAL_OBSERVATIONS_CLASS
from cdisc_rules_engine.enums.variable_roles import VariableRoles
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.expected_variables import ExpectedVariables
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService

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
                        {"name": "AETEST", "ordinal": 1, "core": "Req"},
                        {"name": "AENEW", "ordinal": 2, "core": "Exp"},
                    ],
                }
            ],
        }
    ],
}


@pytest.mark.parametrize("dataset_type", [(PandasDataset), (DaskDataset)])
def test_get_expected_variables(operation_params: OperationParams, dataset_type):
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
    # execute operation
    data_service = LocalDataService.get_instance(
        cache_service=cache, config=ConfigService()
    )
    operation = ExpectedVariables(
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )
    result = operation.execute()
    variables: List[str] = ["AENEW"]
    expected: pd.Series = pd.Series(
        [
            variables,
            variables,
            variables,
        ]
    )
    assert result[operation_params.operation_id].equals(expected)
