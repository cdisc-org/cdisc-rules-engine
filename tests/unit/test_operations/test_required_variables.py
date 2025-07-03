from typing import List
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.config.config import ConfigService

import pandas as pd
import pytest
from unittest.mock import Mock, patch

from cdisc_rules_engine.constants.classes import GENERAL_OBSERVATIONS_CLASS
from cdisc_rules_engine.enums.variable_roles import VariableRoles
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.required_variables import RequiredVariables
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
                {"name": "--TEST", "ordinal": 3},
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
                        {"name": "AETEST", "ordinal": 1, "core": "Req"},
                        {"name": "AENEW", "ordinal": 2, "core": "Exp"},
                    ],
                }
            ],
        }
    ],
}


@pytest.mark.parametrize("dataset_type", [(PandasDataset), (DaskDataset)])
def test_get_required_variables(operation_params: OperationParams, dataset_type):
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
    operation = RequiredVariables(
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
    variables: List[str] = sorted(["STUDYID", "DOMAIN", "AESEQ", "AETEST"])
    for result_array in result[operation_params.operation_id]:
        assert sorted(result_array) == variables
