import pandas as pd
from unittest.mock import MagicMock, patch
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.dataset_builders.dataset_metadata_values_builder import (
    ValueCheckDatasetMetadataDatasetBuilder,
)


@patch(
    "cdisc_rules_engine.dataset_builders.values_dataset_builder.ValuesDatasetBuilder.build"
)
def test_build_with_dataset_metadata(mock_build):
    """Test that the builder correctly attaches dataset metadata to values"""
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
                "Fever",
            ],
        }
    )
    mock_build.return_value = long_df

    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    original_get_metadata = data_service.get_dataset_metadata

    metadata_df = pd.DataFrame(
        [
            {
                "dataset_name": "AE",
                "dataset_label": "Adverse Events",
                "dataset_location": "/path/to/ae.xpt",
                "dataset_size": "1024 KB",
            }
        ]
    )

    data_service.get_dataset_metadata = MagicMock(return_value=metadata_df)

    rule_mock = MagicMock()
    rule_processor_mock = MagicMock()
    rule_processor_mock.get_size_unit_from_rule.return_value = "KB"
    try:
        builder = ValueCheckDatasetMetadataDatasetBuilder(
            rule=rule_mock,
            data_service=data_service,
            cache_service=InMemoryCacheService(),
            rule_processor=rule_processor_mock,
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
        result = builder.build()
        assert data_service.get_dataset_metadata.called
        expected_columns = {
            "row_number",
            "variable_name",
            "variable_value",
            "dataset_name",
            "dataset_label",
            "dataset_location",
            "dataset_size",
        }
        assert set(result.columns.tolist()) == expected_columns
        assert len(result) == 9
        assert all(result["dataset_name"] == "AE")
        assert all(result["dataset_label"] == "Adverse Events")
        assert all(result["dataset_location"] == "/path/to/ae.xpt")
        assert all(result["dataset_size"] == "1024 KB")
        studyid_rows = result[result["variable_name"] == "STUDYID"]
        assert len(studyid_rows) == 3
        assert all(studyid_rows["variable_value"] == "STUDY1")
        assert set(result["row_number"].unique()) == {1, 2, 3}
    finally:
        data_service.get_dataset_metadata = original_get_metadata


@patch(
    "cdisc_rules_engine.dataset_builders.values_dataset_builder.ValuesDatasetBuilder.build"
)
def test_build_split_datasets(mock_build):
    """Test the build_split_datasets method"""
    long_df = pd.DataFrame(
        {
            "row_number": [1, 1, 2, 2],
            "variable_name": ["STUDYID", "USUBJID"] * 2,
            "variable_value": ["STUDY1", "SUBJ1", "STUDY1", "SUBJ2"],
        }
    )
    mock_build.return_value = long_df
    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    original_get_metadata = data_service.get_dataset_metadata

    metadata_df = pd.DataFrame(
        [
            {
                "dataset_name": "DM",
                "dataset_label": "Demographics",
                "dataset_location": "/path/to/dm.xpt",
                "dataset_size": "512 KB",
            }
        ]
    )

    data_service.get_dataset_metadata = MagicMock(return_value=metadata_df)

    rule_mock = MagicMock()
    rule_processor_mock = MagicMock()
    rule_processor_mock.get_size_unit_from_rule.return_value = "KB"
    try:
        builder = ValueCheckDatasetMetadataDatasetBuilder(
            rule=rule_mock,
            data_service=data_service,
            cache_service=InMemoryCacheService(),
            rule_processor=rule_processor_mock,
            data_processor=None,
            dataset_path="",
            datasets=[],
            dataset_metadata=None,
            define_xml_path="",
            standard="",
            standard_version="",
            standard_substandard=None,
        )
        result = builder.build_split_datasets("dm.xpt")

        assert data_service.get_dataset_metadata.called
        expected_columns = {
            "row_number",
            "variable_name",
            "variable_value",
            "dataset_name",
            "dataset_label",
            "dataset_location",
            "dataset_size",
        }
        assert set(result.columns.tolist()) == expected_columns
        assert len(result) == 4
        assert all(result["dataset_name"] == "DM")
        assert all(result["dataset_label"] == "Demographics")
        assert all(result["dataset_location"] == "/path/to/dm.xpt")
        assert all(result["dataset_size"] == "512 KB")
    finally:
        data_service.get_dataset_metadata = original_get_metadata
