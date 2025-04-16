import pandas as pd
from unittest.mock import MagicMock, patch
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.dataset_builders.variables_metadata_values_dataset_builder import (
    ValueCheckVariableMetadataDatasetBuilder,
)


@patch(
    "cdisc_rules_engine.dataset_builders.values_dataset_builder.ValuesDatasetBuilder.build"
)
def test_build_with_variable_metadata(mock_build):
    """Test that the builder correctly attaches variable metadata to values"""
    # Create a df that would be returned by the parent's build method
    long_df = pd.DataFrame(
        {
            "row_number": [1, 1, 1, 2, 2, 2, 3, 3, 3],
            "variable_name": ["STUDYID", "USUBJID", "AETERM"] * 3,
            "variable_value": [
                "STUDY1",
                "SUBJ1",
                "Headache",
                "STUDY1",
                "SUBJ2",
                "Nausea",
                "STUDY1",
                "SUBJ3",
                "Very long text that exceeds 200 characters " * 5,
            ],
        }
    )
    mock_build.return_value = long_df

    # Create the actual data service with necessary attributes
    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())

    # Mock the get_variables_metadata method
    original_get_vars_metadata = data_service.get_variables_metadata
    data_service.get_variables_metadata = MagicMock(
        return_value=pd.DataFrame.from_dict(
            {
                "variable_name": ["STUDYID", "USUBJID", "AETERM"],
                "variable_label": ["Study ID", "Subject ID", "AE Term"],
                "variable_size": [16, 20, 200],
                "variable_order_number": [1, 2, 9],
                "variable_data_type": ["char", "char", "char"],
                "variable_format": ["$16.", "$20.", "$200."],
            }
        )
    )

    # Create builder
    rule_mock = MagicMock()

    try:
        builder = ValueCheckVariableMetadataDatasetBuilder(
            rule=rule_mock,
            data_service=data_service,
            cache_service=InMemoryCacheService(),
            rule_processor=None,
            data_processor=None,
            dataset_path="ae.xpt",
            datasets=[],
            dataset_metadata=SDTMDatasetMetadata(
                name="AE", first_record={"DOMAIN": "AE"}
            ),
            define_xml_path="",
            standard="",
            standard_version="",
            standard_substandard=None,
        )

        # Call the build method
        result = builder.build()

        # Verify the results
        assert data_service.get_variables_metadata.called

        # Check columns
        expected_columns = {
            "row_number",
            "variable_name",
            "variable_value",
            "variable_order_number",
            "variable_label",
            "variable_size",
            "variable_data_type",
            "variable_format",
            "variable_value_length",
        }
        assert set(result.columns.tolist()) == expected_columns

        # Check row count (3 rows * 3 variables = 9 rows in long format)
        assert len(result) == 9

        # Check variable metadata is attached to each row
        studyid_rows = result[result["variable_name"] == "STUDYID"]
        assert len(studyid_rows) == 3
        assert all(studyid_rows["variable_label"] == "Study ID")
        assert all(studyid_rows["variable_size"] == 16)
        assert all(studyid_rows["variable_data_type"] == "char")
        assert all(studyid_rows["variable_order_number"] == 1)

        # Check value length calculation is correct
        for _, row in result.iterrows():
            if row["variable_name"] == "STUDYID":
                assert row["variable_value_length"] == 6  # "STUDY1" length
            elif row["variable_name"] == "USUBJID":
                assert row["variable_value_length"] == len(row["variable_value"])
            elif row["variable_name"] == "AETERM" and row["row_number"] == 3:
                # Check the long text value
                assert (
                    row["variable_value_length"] > 200
                )  # Should exceed the 200 char limit

    finally:
        # Restore the original method
        data_service.get_variables_metadata = original_get_vars_metadata


@patch(
    "cdisc_rules_engine.dataset_builders.values_dataset_builder.ValuesDatasetBuilder.build_split_datasets"
)
def test_build_split_datasets(mock_build_split_datasets):
    """Test the build_split_datasets method"""
    # Create a df that would be returned by the parent's build_split_datasets method
    long_df = pd.DataFrame(
        {
            "row_number": [1, 1, 1, 2, 2, 2],
            "variable_name": ["STUDYID", "AGE", "SEX"] * 2,
            "variable_value": ["STUDY1", 25, "M", "STUDY1", 42, "F"],
        }
    )
    mock_build_split_datasets.return_value = long_df

    # Create the actual data service with necessary attributes
    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())

    # Mock the get_variables_metadata method
    original_get_vars_metadata = data_service.get_variables_metadata
    data_service.get_variables_metadata = MagicMock(
        return_value=pd.DataFrame.from_dict(
            {
                "variable_name": ["STUDYID", "AGE", "SEX"],
                "variable_label": ["Study ID", "Age", "Sex"],
                "variable_size": [16, 8, 1],
                "variable_order_number": [1, 4, 5],
                "variable_data_type": ["char", "num", "char"],
                "variable_format": ["$16.", "8.", "$1."],
            }
        )
    )

    # Create builder
    rule_mock = MagicMock()

    try:
        builder = ValueCheckVariableMetadataDatasetBuilder(
            rule=rule_mock,
            data_service=data_service,
            cache_service=InMemoryCacheService(),
            rule_processor=None,
            data_processor=None,
            dataset_path="",
            datasets=[],
            dataset_metadata=None,
            define_xml_path="",
            standard="",
            standard_version="",
            standard_substandard=None,
        )

        # Call the build_split_datasets method
        result = builder.build_split_datasets("dm.xpt")

        # Verify the results
        assert data_service.get_variables_metadata.called
        assert mock_build_split_datasets.called

        # Check columns
        expected_columns = {
            "row_number",
            "variable_name",
            "variable_value",
            "variable_order_number",
            "variable_label",
            "variable_size",
            "variable_data_type",
            "variable_format",
            "variable_value_length",
        }
        assert set(result.columns.tolist()) == expected_columns

        # Check row count (2 rows * 3 variables = 6 rows in long format)
        assert len(result) == 6

        # Check numeric vs character variable length calculation
        age_rows = result[result["variable_name"] == "AGE"]
        sex_rows = result[result["variable_name"] == "SEX"]

        # For numeric variables, length should be 0
        assert all(age_rows["variable_value_length"] == 0)

        # For character variables, length should match string length
        assert all(
            sex_rows["variable_value_length"] == 1
        )  # "M" and "F" are both length 1

    finally:
        # Restore the original method
        data_service.get_variables_metadata = original_get_vars_metadata
