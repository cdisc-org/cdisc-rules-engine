from unittest.mock import MagicMock, patch
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
from cdisc_rules_engine.utilities.utils import get_standard_details_cache_key
import pandas as pd
from cdisc_rules_engine.dataset_builders.variables_metadata_with_library_metadata import (  # noqa: E501
    VariablesMetadataWithLibraryMetadataDatasetBuilder,
)
from cdisc_rules_engine.services.data_services import LocalDataService


@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
)
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_variables_metadata",
)
def test_variable_metadata_with_library_metadata_dataset_builder(
    mock_get_variables_metadata: MagicMock, mock_get_dataset: MagicMock
):
    mock_get_variables_metadata.return_value = pd.DataFrame.from_dict(
        {
            "variable_name": ["STUDYID", "USUBJID", "AETERM"],
            "variable_label": ["A", "B", "C"],
            "variable_size": [16, 16, 8],
            "variable_order_number": [1, 2, 9],
            "variable_data_type": ["Char", "Char", "Char"],
        }
    )

    mock_get_dataset.return_value = pd.DataFrame.from_dict(
        {
            "STUDYID": ["A", "B", "C"],
            "USUBJID": ["", "A", "B"],
            "AETERM": ["", "C", "A"],
        }
    )
    cache = InMemoryCacheService()
    standard = "sdtmig"
    standard_version = "3-4"
    cache_key = get_standard_details_cache_key(standard, standard_version)
    cache.add(
        cache_key,
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
                                {
                                    "name": "STUDYID",
                                    "ordinal": "1",
                                    "role": "Identifier",
                                    "label": "Reported Term for the adverse event",
                                    "simpleDatatype": "Char",
                                    "core": "Req",
                                },
                                {
                                    "name": "USUBJID",
                                    "ordinal": "2",
                                    "role": "Identifier",
                                    "label": "Reported Term for the adverse event",
                                    "simpleDatatype": "Char",
                                    "core": "Req",
                                },
                                {
                                    "name": "AETERM",
                                    "ordinal": "9",
                                    "role": "Topic",
                                    "label": "Reported Term for the adverse event",
                                    "simpleDatatype": "Char",
                                    "core": "Req",
                                },
                                {
                                    "name": "AESEQ",
                                    "ordinal": "8",
                                    "role": "Topic",
                                    "label": "Sequence Number",
                                    "simpleDatatype": "Num",
                                    "core": "Req",
                                },
                            ],
                        }
                    ],
                }
            ],
        },
    )
    result = VariablesMetadataWithLibraryMetadataDatasetBuilder(
        rule=None,
        data_service=LocalDataService(MagicMock(), MagicMock(), MagicMock()),
        cache_service=cache,
        rule_processor=None,
        data_processor=None,
        dataset_path=None,
        datasets=[],
        domain="AE",
        define_xml_path=None,
        standard=standard,
        standard_version=standard_version,
    ).build()
    assert result.columns.tolist() == [
        "variable_name",
        "variable_label",
        "variable_size",
        "variable_order_number",
        "variable_data_type",
        "library_variable_name",
        "library_variable_role",
        "library_variable_label",
        "library_variable_core",
        "library_variable_order_number",
        "library_variable_data_type",
        "has_empty_values",
    ]
    assert result["library_variable_name"].tolist() == [
        "STUDYID",
        "USUBJID",
        "AETERM",
        "AESEQ",
    ]
    assert result["variable_name"].tolist() == ["STUDYID", "USUBJID", "AETERM", ""]
    assert result["has_empty_values"].tolist() == [False, True, True, True]
