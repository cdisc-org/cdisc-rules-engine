from unittest.mock import MagicMock, patch
from cdisc_rules_engine.dataset_builders.variables_metadata_with_define_and_library_dataset_builder import (
    VariablesMetadataWithDefineAndLibraryDatasetBuilder,
)
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
import pandas as pd


@patch(
    "cdisc_rules_engine.services.define_xml.define_xml_reader_factory.DefineXMLReaderFactory._get_define_xml_reader"
)
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
)
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_variables_metadata",
)
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_define_xml_contents",
)
def test_build_combined_metadata(
    mock_get_define_xml,
    mock_get_variables_metadata,
    mock_get_dataset,
    mock_get_define_reader,
):
    """Test that the builder correctly combines all three metadata sources"""
    # Use the actual define.xml content
    with open("tests/resources/define.xml", "rb") as f:
        define_xml_content = f.read()
    mock_get_define_xml.return_value = define_xml_content

    # Pre-format variables metadata as DataFrame
    mock_get_variables_metadata.return_value = pd.DataFrame.from_dict(
        {
            "variable_name": ["STUDYID", "USUBJID", "AETERM"],
            "variable_label": ["Study ID", "Subject ID", "AE Term"],
            "variable_size": [16, 16, 200],
            "variable_order_number": [1, 2, 9],
            "variable_data_type": ["Char", "Char", "Char"],
        }
    )

    # Pre-format dataset contents as DataFrame
    mock_get_dataset.return_value = pd.DataFrame.from_dict(
        {
            "STUDYID": ["STUDY1", "STUDY1", "STUDY1"],
            "USUBJID": ["SUBJ1", "", "SUBJ3"],
            "AETERM": ["Headache", "Nausea", ""],
        }
    )

    # Create the define metadata directly as a dict list (not DataFrame)
    define_metadata = [
        {
            "define_variable_name": "STUDYID",
            "define_variable_label": "Study Identifier",
            "define_variable_data_type": "Char",
            "define_variable_role": "Identifier",
            "define_variable_size": 16,
            "define_variable_is_collected": False,
            "define_variable_has_no_data": False,
            "define_variable_order_number": 1,
        },
        {
            "define_variable_name": "AETERM",
            "define_variable_label": "Reported Term for the Adverse Event",
            "define_variable_data_type": "Char",
            "define_variable_role": "Topic",
            "define_variable_size": 200,
            "define_variable_is_collected": True,
            "define_variable_has_no_data": False,
            "define_variable_order_number": 9,
        },
    ]

    # Create a reader that returns plain Python objects, not mocks
    mock_reader = MagicMock()
    mock_reader.extract_variables_metadata = lambda: define_metadata
    mock_get_define_reader.return_value = mock_reader

    # Setup library metadata
    standard_data = {
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
                                "label": "Study Identifier",
                                "simpleDatatype": "Char",
                                "core": "Req",
                            },
                            {
                                "name": "USUBJID",
                                "ordinal": "2",
                                "role": "Identifier",
                                "label": "Unique Subject Identifier",
                                "simpleDatatype": "Char",
                                "core": "Req",
                            },
                            {
                                "name": "AETERM",
                                "ordinal": "9",
                                "role": "Topic",
                                "label": "Reported Term for the Adverse Event",
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

    # Create builder instance
    builder = VariablesMetadataWithDefineAndLibraryDatasetBuilder(
        rule=None,
        data_service=LocalDataService(MagicMock(), MagicMock(), MagicMock()),
        cache_service=InMemoryCacheService(),
        rule_processor=None,
        data_processor=None,
        dataset_path="test/path",
        datasets=[],
        domain="AE",
        define_xml_path="tests/resources/define.xml",
        standard="sdtmig",
        standard_version="3-4",
        library_metadata=library_metadata,
    )

    result = builder.build()

    # Verify results
    assert set(result.columns.tolist()) == {
        "variable_name",
        "variable_label",
        "variable_size",
        "variable_order_number",
        "variable_data_type",
        "library_variable_name",
        "library_variable_role",
        "library_variable_label",
        "library_variable_core",
        "library_variable_data_type",
        "define_variable_name",
        "define_variable_label",
        "define_variable_data_type",
        "define_variable_size",
        "define_variable_is_collected",
        "define_variable_role",
        "variable_has_empty_values",
    }

    # Verify library metadata integration
    assert result["library_variable_name"].tolist() == [
        "STUDYID",
        "USUBJID",
        "AETERM",
        "AESEQ",
    ]
    assert result["variable_name"].tolist() == ["STUDYID", "USUBJID", "AETERM", ""]

    # Verify empty values flag
    assert result["variable_has_empty_values"].tolist() == [False, True, True, True]


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_variables_metadata"
)
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_define_xml_contents"
)
def test_metadata_mismatch_handling(
    mock_get_define_xml: MagicMock,
    mock_get_variables_metadata: MagicMock,
    mock_get_dataset: MagicMock,
    mock_data_service,
    mock_variables_metadata,
    mock_define_xml_data,
    mock_library_metadata,
    mock_dataset_contents,
):
    """Test handling of mismatched metadata between sources"""

    # Modify variables metadata to create a mismatch
    modified_variables_metadata = mock_variables_metadata.copy()
    modified_variables_metadata.loc[
        0, "variable_size"
    ] = 32  # Different from define XML

    mock_get_variables_metadata.return_value = modified_variables_metadata
    mock_get_dataset.return_value = mock_dataset_contents
    mock_get_define_xml.return_value = mock_define_xml_data

    builder = VariablesMetadataWithDefineAndLibraryDatasetBuilder(
        rule=None,
        data_service=LocalDataService(MagicMock(), MagicMock(), MagicMock()),
        cache_service=InMemoryCacheService(),
        rule_processor=None,
        data_processor=None,
        dataset_path="test/path",
        datasets=[],
        domain="AE",
        define_xml_path="test/define.xml",
        standard="sdtmig",
        standard_version="3-4",
        library_metadata=mock_library_metadata,
    )

    result = builder.build()

    studyid_row = result[result["variable_name"] == "STUDYID"].iloc[0]
    assert studyid_row["variable_size"] == 32
    assert studyid_row["define_variable_size"] == 16


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_variables_metadata"
)
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_define_xml_contents"
)
def test_missing_variable_handling(
    mock_get_define_xml: MagicMock,
    mock_get_variables_metadata: MagicMock,
    mock_get_dataset: MagicMock,
    mock_data_service,
    mock_variables_metadata,
    mock_define_xml_metadata,
    mock_library_metadata,
    mock_dataset_contents,
):
    """Test handling of variables missing from some sources"""

    # Add a variable that's only in library metadata
    extra_library_variable = "AENEWVAR"

    # Modify library metadata to include the extra variable
    modified_library_metadata = mock_library_metadata
    modified_library_metadata.standard_metadata["classes"][0]["datasets"][0][
        "datasetVariables"
    ].append(
        {
            "name": extra_library_variable,
            "ordinal": "10",
            "role": "Topic",
            "label": "New Variable",
            "simpleDatatype": "Char",
            "core": "Perm",
        }
    )

    mock_get_variables_metadata.return_value = mock_variables_metadata
    mock_get_dataset.return_value = mock_dataset_contents
    mock_get_define_xml.return_value = mock_define_xml_metadata

    builder = VariablesMetadataWithDefineAndLibraryDatasetBuilder(
        rule=None,
        data_service=LocalDataService(MagicMock(), MagicMock(), MagicMock()),
        cache_service=InMemoryCacheService(),
        rule_processor=None,
        data_processor=None,
        dataset_path="test/path",
        datasets=[],
        domain="AE",
        define_xml_path="test/define.xml",
        standard="sdtmig",
        standard_version="3-4",
        library_metadata=modified_library_metadata,
    )

    result = builder.build()

    # Verify handling of the library-only variable
    library_only_row = result[result["library_variable_name"] == extra_library_variable]
    assert not library_only_row.empty
    assert library_only_row.iloc[0]["variable_name"] == ""
    assert library_only_row.iloc[0]["define_variable_name"] == ""
    assert library_only_row.iloc[0]["library_variable_core"] == "Perm"
