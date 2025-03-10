from unittest.mock import MagicMock, patch
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
from cdisc_rules_engine.services.data_services import LocalDataService
from pathlib import Path
import pandas as pd
import numpy as np
from cdisc_rules_engine.dataset_builders.variables_metadata_with_define_and_library_dataset_builder import (
    VariablesMetadataWithDefineAndLibraryDatasetBuilder,
)

resources_path: Path = Path(__file__).parent.parent.parent.joinpath("resources")
test_define_file_path: Path = resources_path.joinpath("test_defineV22-SDTM.xml")


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_variables_metadata"
)
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_define_xml_contents"
)
@patch(
    "cdisc_rules_engine.dataset_builders.variables_metadata_with_define_and_library_dataset_builder"
    ".VariablesMetadataWithDefineAndLibraryDatasetBuilder.get_library_variables_metadata"
)
def test_build_combined_metadata(
    mock_get_library_variables_metadata,
    mock_get_define_xml,
    mock_get_variables_metadata,
    mock_get_dataset,
):
    """Test that the builder correctly combines all three metadata sources"""
    # Load actual define XML content
    with open(test_define_file_path, "rb") as f:
        define_data = f.read()
    mock_get_define_xml.return_value = define_data

    # Setup variables metadata
    mock_get_variables_metadata.return_value = pd.DataFrame.from_dict(
        {
            "variable_name": ["STUDYID", "USUBJID", "AETERM"],
            "variable_label": ["Study ID", "Subject ID", "AE Term"],
            "variable_size": [16, 16, 200],
            "variable_order_number": [1, 2, 9],
            "variable_data_type": ["Char", "Char", "Char"],
        }
    )

    # Setup dataset contents
    mock_get_dataset.return_value = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "STUDYID": ["STUDY1", "STUDY1", "STUDY1"],
                "USUBJID": ["SUBJ1", "", "SUBJ3"],
                "AETERM": ["Headache", "Nausea", ""],
            }
        )
    )

    # Create mock library variables metadata
    library_vars_data = pd.DataFrame(
        {
            "library_variable_name": ["STUDYID", "USUBJID", "AETERM", "AESEQ"],
            "library_variable_role": ["Identifier", "Identifier", "Topic", "Topic"],
            "library_variable_label": [
                "Study Identifier",
                "Unique Subject Identifier",
                "Reported Term for the Adverse Event",
                "Sequence Number",
            ],
            "library_variable_core": ["Req", "Req", "Req", "Req"],
            "library_variable_order_number": ["1", "2", "9", "8"],
            "library_variable_data_type": ["Char", "Char", "Char", "Num"],
        }
    )
    mock_get_library_variables_metadata.return_value = PandasDataset(library_vars_data)

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

    # Create builder
    builder = VariablesMetadataWithDefineAndLibraryDatasetBuilder(
        rule=None,
        data_service=LocalDataService(MagicMock(), MagicMock(), MagicMock()),
        cache_service=InMemoryCacheService(),
        rule_processor=None,
        data_processor=None,
        dataset_path=str(test_define_file_path),
        datasets=[],
        dataset_metadata=SDTMDatasetMetadata(name="AE", first_record={"DOMAIN": "AE"}),
        define_xml_path=str(test_define_file_path),
        standard="sdtmig",
        standard_version="3-4",
        standard_substandard=None,
        library_metadata=library_metadata,
    )

    result = builder.build()

    expected_columns = {
        "variable_name",
        "variable_label",
        "variable_size",
        "variable_order_number",
        "variable_data_type",
        "define_variable_name",
        "define_variable_label",
        "define_variable_data_type",
        "define_variable_is_collected",
        "define_variable_role",
        "define_variable_size",
        "define_variable_ccode",
        "define_variable_format",
        "define_variable_allowed_terms",
        "define_variable_origin_type",
        "define_variable_has_no_data",
        "define_variable_order_number",
        "define_variable_length",
        "define_variable_has_codelist",
        "define_variable_codelist_coded_values",
        "define_variable_mandatory",
        "define_variable_has_comment",
        "library_variable_name",
        "library_variable_role",
        "library_variable_label",
        "library_variable_core",
        "library_variable_order_number",
        "library_variable_data_type",
        "variable_has_empty_values",
    }
    assert set(result.columns.tolist()) == expected_columns
    core_variables = ["STUDYID", "USUBJID", "AETERM"]
    for var in core_variables:
        row = result[result["variable_name"] == var].iloc[0]
        assert row["define_variable_name"] == var
        assert row["library_variable_name"] == var
        assert row["variable_has_empty_values"] == (var in ["USUBJID", "AETERM"])
    studyid_row = result[result["variable_name"] == "STUDYID"].iloc[0]
    assert studyid_row["variable_size"] == 16.0
    assert studyid_row["variable_order_number"] == 1.0
    assert studyid_row["variable_data_type"] == "Char"
    assert studyid_row["define_variable_role"] == "Identifier"
    assert studyid_row["library_variable_core"] == "Req"
    assert not studyid_row["variable_has_empty_values"]
    define_only_vars = ["DOMAIN", "AESEQ", "AELNKID"]  # Sample of define-only variables
    for var in define_only_vars:
        rows = result[result["define_variable_name"] == var]
        assert len(rows) == 1
        row = rows.iloc[0]
        assert pd.isna(row["variable_name"]) or row["variable_name"] == ""
        assert row["variable_has_empty_values"]
    library_only_rows = result[
        (result["library_variable_name"].notna())
        & (result["variable_name"].isna() | (result["variable_name"] == ""))
    ]
    assert not library_only_rows.empty
    aeseq_lib_row = result[result["library_variable_name"] == "AESEQ"].iloc[0]
    assert aeseq_lib_row["library_variable_role"] == "Topic"
    assert aeseq_lib_row["library_variable_core"] == "Req"
    assert aeseq_lib_row["library_variable_data_type"] == "Num"
    codelist_vars = result[result["define_variable_has_codelist"].astype(bool)]
    assert not codelist_vars.empty
    aesev_row = result[result["define_variable_name"] == "AESEV"].iloc[0]
    assert aesev_row["define_variable_has_codelist"]
    assert set(aesev_row["define_variable_codelist_coded_values"]) == {
        "MILD",
        "MODERATE",
        "SEVERE",
    }
    mandatory_vars = result[result["define_variable_mandatory"] == "Yes"]
    assert not mandatory_vars.empty
    assert all(
        mandatory_vars["define_variable_name"].isin(
            ["STUDYID", "USUBJID", "AETERM", "DOMAIN", "AESEQ", "AEDECOD"]
        )
    )
    empty_value_vars = result[result["variable_has_empty_values"]]
    assert not empty_value_vars.empty
    assert "USUBJID" in empty_value_vars["variable_name"].values
    assert "AETERM" in empty_value_vars["variable_name"].values
    numeric_vars = result[result["define_variable_data_type"] == "integer"]
    assert all(
        var in ["AESEQ", "AESTDY", "AEENDY"]
        for var in numeric_vars["define_variable_name"]
    )
    non_empty_vars = ["STUDYID"]
    for _, row in result.iterrows():
        if row["variable_name"] in non_empty_vars:
            assert row["variable_has_empty_values"] is False
        else:
            assert row["variable_has_empty_values"]
    assert not result["variable_name"].isin([np.nan]).any()
    assert not result["define_variable_name"].isin([np.nan]).any()
    assert not result["library_variable_name"].isin([np.nan]).any()
