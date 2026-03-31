from unittest.mock import MagicMock, patch
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
from cdisc_rules_engine.services.data_services import LocalDataService
from pathlib import Path
import pandas as pd
from cdisc_rules_engine.dataset_builders.variables_metadata_with_define_dataset_builder import (
    VariablesMetadataWithDefineDatasetBuilder,
)

resources_path: Path = Path(__file__).parent.parent.parent.joinpath("resources")
test_define_file_path: Path = resources_path.joinpath("test_defineV21-SDTM.xml")


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_variables_metadata"
)
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_define_xml_contents"
)
def test_build_combined_metadata(
    mock_get_define_xml,
    mock_get_variables_metadata,
    mock_get_dataset,
):
    """Test that the builder correctly combines two metadata sources"""
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

    # Create builder
    builder = VariablesMetadataWithDefineDatasetBuilder(
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
        "define_variable_codelist_coded_codes",
        "define_variable_mandatory",
        "define_variable_has_comment",
        "define_variable_has_method",
        "variable_has_empty_values",
        "variable_is_empty",
    }
    assert set(result.columns.tolist()) == expected_columns

    core_variables = ["STUDYID", "USUBJID", "AETERM"]
    for var in core_variables:
        row = result[result["variable_name"] == var].iloc[0]
        assert row["define_variable_name"] == var
        assert row["variable_has_empty_values"] == (var in ["USUBJID", "AETERM"])

    studyid_row = result[result["variable_name"] == "STUDYID"].iloc[0]
    assert studyid_row["variable_size"] == 16.0
    assert studyid_row["variable_order_number"] == 1.0
    assert studyid_row["variable_data_type"] == "Char"
    assert studyid_row["define_variable_role"] == "Identifier"
    assert not studyid_row["variable_has_empty_values"]
    assert not studyid_row["variable_is_empty"]

    usubjid_row = result[result["variable_name"] == "USUBJID"].iloc[0]
    assert usubjid_row["variable_size"] == 16.0
    assert usubjid_row["variable_order_number"] == 2.0
    assert usubjid_row["variable_data_type"] == "Char"
    assert usubjid_row["define_variable_role"] == "Identifier"
    assert usubjid_row["variable_has_empty_values"]
    assert not usubjid_row["variable_is_empty"]

    aeterm_row = result[result["variable_name"] == "AETERM"].iloc[0]
    assert aeterm_row["variable_size"] == 200.0
    assert aeterm_row["variable_order_number"] == 9.0
    assert aeterm_row["variable_data_type"] == "Char"
    assert aeterm_row["define_variable_role"] == "Topic"
    assert aeterm_row["variable_has_empty_values"]
    assert not aeterm_row["variable_is_empty"]

    # We need to check that the rest of the variables are coming from define.xml (variable_name is NaN)
    assert len(result[
        (result["variable_name"] == "") & result["define_variable_name"].notna()
    ]) == 34

    assert len(result) == 37
