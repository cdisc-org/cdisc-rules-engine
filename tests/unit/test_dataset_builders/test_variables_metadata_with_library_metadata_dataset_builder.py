from unittest.mock import MagicMock, patch
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.dummy_models.dummy_dataset import DummyDataset
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
import pandas as pd
from cdisc_rules_engine.dataset_builders.variables_metadata_with_library_metadata import (  # noqa: E501
    VariablesMetadataWithLibraryMetadataDatasetBuilder,
)
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.constants.classes import GENERAL_OBSERVATIONS_CLASS
from cdisc_rules_engine.enums.variable_roles import VariableRoles


@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
)
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_variables_metadata",
)
@patch(
    "cdisc_rules_engine.dataset_builders.variables_metadata_with_library_metadata"
    ".VariablesMetadataWithLibraryMetadataDatasetBuilder.get_library_variables_metadata"
)
def test_variable_metadata_with_library_metadata_dataset_builder(
    mock_get_library_variables_metadata: MagicMock,
    mock_get_variables_metadata: MagicMock,
    mock_get_dataset: MagicMock,
):
    mock_get_variables_metadata.return_value = pd.DataFrame.from_dict(
        {
            "variable_name": ["STUDYID", "USUBJID", "AETERM", "AESEQ"],
            "variable_label": ["A", "B", "C", "D"],
            "variable_size": [16, 16, 8, 8],
            "variable_order_number": [1, 2, 9, 8],
            "variable_data_type": ["Char", "Char", "Char", "Num"],
        }
    )

    library_vars_data = pd.DataFrame(
        {
            "library_variable_name": ["STUDYID", "USUBJID", "AETERM", "AESEQ"],
            "library_variable_role": ["Identifier", "Identifier", "Topic", "Topic"],
            "library_variable_label": [
                "Study Identifier",
                "Unique Subject Identifier",
                "Reported Term for the adverse event",
                "Sequence Number",
            ],
            "library_variable_core": ["Req", "Req", "Req", "Req"],
            "library_variable_order_number": ["1", "2", "9", "8"],
            "library_variable_data_type": ["Char", "Char", "Char", "Num"],
        }
    )
    mock_get_library_variables_metadata.return_value = PandasDataset(library_vars_data)

    mock_get_dataset.return_value = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "STUDYID": ["A", "B", "C"],
                "USUBJID": ["", "A", "B"],
                "AETERM": ["", "C", "A"],
                "AESEQ": [1, 2, 3],
            }
        )
    )
    cache = InMemoryCacheService()
    standard = "sdtmig"
    standard_version = "3-4"
    standard_substandard = None
    standard_data = {
        "_links": {"model": {"href": "/mdr/sdtm/1-5"}},
        "domains": ["AE", "DM", "VS"],
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
    }
    library_metadata = LibraryMetadataContainer(standard_metadata=standard_data)
    result = VariablesMetadataWithLibraryMetadataDatasetBuilder(
        rule=None,
        data_service=LocalDataService(MagicMock(), MagicMock(), MagicMock()),
        cache_service=cache,
        rule_processor=None,
        data_processor=None,
        dataset_path=None,
        datasets=[],
        dataset_metadata=DummyDataset(
            {
                "filename": "ae.xpt",
                "label": "Adverse Events",
                "variables": [
                    {
                        "name": "DOMAIN",
                        "label": "Domain Abbreviation",
                        "type": "Char",
                        "length": 4,
                    },
                ],
                "records": {"DOMAIN": ["AE"]},
            }
        ),
        define_xml_path=None,
        standard=standard,
        standard_version=standard_version,
        standard_substandard=standard_substandard,
        library_metadata=library_metadata,
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
        "variable_has_empty_values",
    ]
    assert result["library_variable_name"].tolist() == [
        "STUDYID",
        "USUBJID",
        "AETERM",
        "AESEQ",
    ]
    assert result["variable_name"].tolist() == ["STUDYID", "USUBJID", "AETERM", "AESEQ"]
    assert result["variable_has_empty_values"].tolist() == [False, True, True, False]


@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
)
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_variables_metadata",
)
@patch(
    "cdisc_rules_engine.dataset_builders.variables_metadata_with_library_metadata"
    ".VariablesMetadataWithLibraryMetadataDatasetBuilder.get_library_variables_metadata"
)
def test_variable_metadata_with_library_metadata_dataset_builder_variable_only_in_model(
    mock_get_library_variables_metadata: MagicMock,
    mock_get_variables_metadata: MagicMock,
    mock_get_dataset: MagicMock,
):
    mock_get_variables_metadata.return_value = pd.DataFrame.from_dict(
        {
            "variable_name": ["STUDYID", "USUBJID", "AETERM", "AEMODELVAR"],
            "variable_label": ["A", "B", "C", "A"],
            "variable_size": [16, 16, 8, 8],
            "variable_order_number": [1, 2, 9, 2000],
            "variable_data_type": ["Char", "Char", "Char", "Num"],
        }
    )
    library_vars_data = pd.DataFrame(
        {
            "library_variable_name": ["STUDYID", "USUBJID", "AETERM", "AEMODELVAR"],
            "library_variable_role": ["Identifier", "Identifier", "Topic", "Timing"],
            "library_variable_label": [
                "Study Identifier",
                "Unique Subject Identifier",
                "Reported Term for the adverse event",
                "Model Variable",
            ],
            "library_variable_core": ["Req", "Req", "Req", "Perm"],
            "library_variable_order_number": ["1", "2", "9", "2000"],
            "library_variable_data_type": ["Char", "Char", "Char", "Num"],
        }
    )
    mock_get_library_variables_metadata.return_value = PandasDataset(library_vars_data)
    mock_get_dataset.return_value = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "STUDYID": ["A", "B", "C"],
                "USUBJID": ["", "A", "B"],
                "AETERM": ["", "C", "A"],
                "AEMODELVAR": ["C", "D", "A"],
            }
        )
    )
    cache = InMemoryCacheService()
    standard = "sdtmig"
    standard_version = "3-4"
    standard_substandard = None
    standard_data = {
        "_links": {"model": {"href": "/mdr/sdtm/2-0"}},
        "domains": ["AE", "DM", "VS"],
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
    }

    model_metadata = {
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
                        "simpleDatatype": "Char",
                    },
                    {
                        "name": "STUDYID",
                        "role": VariableRoles.IDENTIFIER.value,
                        "ordinal": 1,
                        "simpleDatatype": "Char",
                    },
                    {
                        "name": "TIMING_VAR",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 33,
                        "simpleDatatype": "Char",
                    },
                    {
                        "name": "--MODELVAR",
                        "simpleDatatype": "Num",
                        "role": VariableRoles.TIMING.value,
                        "ordinal": 2000,
                    },
                ],
            },
        ],
    }
    library_metadata = LibraryMetadataContainer(
        standard_metadata=standard_data, model_metadata=model_metadata
    )
    result = VariablesMetadataWithLibraryMetadataDatasetBuilder(
        rule=None,
        data_service=LocalDataService(MagicMock(), MagicMock(), MagicMock()),
        cache_service=cache,
        rule_processor=None,
        data_processor=None,
        dataset_path=None,
        datasets=[],
        dataset_metadata=DummyDataset(
            {
                "filename": "ae.xpt",
                "label": "Adverse Events",
                "variables": [
                    {
                        "name": "DOMAIN",
                        "label": "Domain Abbreviation",
                        "type": "Char",
                        "length": 4,
                    },
                ],
                "records": {"DOMAIN": ["AE"]},
            }
        ),
        define_xml_path=None,
        standard=standard,
        standard_version=standard_version,
        standard_substandard=standard_substandard,
        library_metadata=library_metadata,
    ).build()
    assert set(result.columns.tolist()) == set(
        [
            "variable_name",
            "variable_label",
            "variable_size",
            "variable_order_number",
            "variable_data_type",
            "library_variable_name",
            "library_variable_role",
            "library_variable_order_number",
            "library_variable_label",
            "library_variable_core",
            "library_variable_data_type",
            "variable_has_empty_values",
        ]
    )
    assert result["library_variable_name"].tolist() == [
        "STUDYID",
        "USUBJID",
        "AETERM",
        "AEMODELVAR",
    ]
    assert result["variable_name"].tolist() == [
        "STUDYID",
        "USUBJID",
        "AETERM",
        "AEMODELVAR",
    ]
    assert result["variable_has_empty_values"].tolist() == [
        False,
        True,
        True,
        False,
    ]
