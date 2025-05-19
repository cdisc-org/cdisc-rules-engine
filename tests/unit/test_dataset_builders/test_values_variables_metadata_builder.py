from unittest.mock import MagicMock, patch
import pandas as pd
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
from cdisc_rules_engine.dataset_builders.variables_metadata_values_dataset_builder import (
    ValueCheckVariableMetadataDatasetBuilder,
)
from cdisc_rules_engine.services.data_services.local_data_service import (
    LocalDataService,
)
from cdisc_rules_engine.models.dataset import PandasDataset


@patch(
    "cdisc_rules_engine.dataset_builders.values_dataset_builder.ValuesDatasetBuilder.build"
)
def test_build_with_variable_metadata(mock_build):
    """Test that the builder correctly attaches variable metadata to values"""
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
    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    original_get_vars_metadata = data_service.get_variables_metadata
    data_service.get_variables_metadata = MagicMock(
        return_value=pd.DataFrame.from_dict(
            {
                "variable_name": ["STUDYID", "USUBJID", "AETERM"],
                "variable_label": ["Study ID", "Subject ID", "AE Term"],
                "variable_size": [16, 20, 200],
                "variable_order_number": [1, 2, 9],
                "variable_data_type": ["text", "text", "text"],
                "variable_format": ["$16.", "$20.", "$200."],
            }
        )
    )

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
        result = builder.build()
        assert data_service.get_variables_metadata.called
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
        assert len(result) == 9
        studyid_rows = result[result["variable_name"] == "STUDYID"]
        assert len(studyid_rows) == 3
        assert all(studyid_rows["variable_label"] == "Study ID")
        assert all(studyid_rows["variable_size"] == 16)
        assert all(studyid_rows["variable_data_type"] == "text")
        assert all(studyid_rows["variable_order_number"] == 1)
        for _, row in result.iterrows():
            if row["variable_name"] == "STUDYID":
                assert row["variable_value_length"] == 6
            elif row["variable_name"] == "USUBJID":
                assert row["variable_value_length"] == len(row["variable_value"])
            elif row["variable_name"] == "AETERM" and row["row_number"] == 3:
                assert row["variable_value_length"] > 200
    finally:
        data_service.get_variables_metadata = original_get_vars_metadata


def test_concat_with_split_datasets():
    # Test data
    ae1_data = pd.DataFrame(
        {
            "row_number": [1, 2, 3],
            "STUDYID": ["STUDY1", "STUDY1", "STUDY1"],
            "USUBJID": ["SUBJ-001", "SUBJ-002", "SUBJ-003"],
            "AETERM": ["Headache", "Fever", "Nausea"],
        }
    )
    ae2_data = pd.DataFrame(
        {
            "row_number": [4, 5, 6],
            "STUDYID": ["STUDY1", "STUDY1", "STUDY1"],
            "USUBJID": ["SUBJ-004", "SUBJ-005", "SUBJ-006"],
            "AETERM": ["Dizziness", "Rash", "Fatigue"],
        }
    )
    ae1_metadata = SDTMDatasetMetadata(
        name="AE1",
        full_path="ae1.xpt",
        filename="ae1.xpt",
        first_record={"DOMAIN": "AE"},
    )
    ae2_metadata = SDTMDatasetMetadata(
        name="AE2",
        full_path="ae2.xpt",
        filename="ae2.xpt",
        first_record={"DOMAIN": "AE"},
    )

    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    data_service.get_dataset = MagicMock(
        side_effect=lambda dataset_name, **kwargs: PandasDataset(
            ae1_data if dataset_name == "ae1.xpt" else ae2_data
        )
    )
    data_service.get_variables_metadata = MagicMock(
        return_value=pd.DataFrame.from_dict(
            {
                "variable_name": ["STUDYID", "USUBJID", "AETERM"],
                "variable_label": ["Study ID", "Subject ID", "AE Term"],
                "variable_size": [16, 20, 200],
                "variable_order_number": [1, 2, 9],
                "variable_data_type": ["text", "text", "text"],
                "variable_format": ["$16.", "$20.", "$200."],
            }
        )
    )

    builder = ValueCheckVariableMetadataDatasetBuilder(
        rule=MagicMock(),
        data_service=data_service,
        cache_service=InMemoryCacheService(),
        rule_processor=None,
        data_processor=None,
        dataset_path="ae.xpt",
        datasets=[],
        dataset_metadata=SDTMDatasetMetadata(name="AE", first_record={"DOMAIN": "AE"}),
        define_xml_path="",
        standard="",
        standard_version="",
        standard_substandard=None,
    )
    result = data_service.concat_split_datasets(
        func_to_call=builder.build_split_datasets,
        datasets_metadata=[ae1_metadata, ae2_metadata],
    )
    expected_data = pd.DataFrame(
        {
            "row_number": [
                1,
                2,
                3,
                1,
                2,
                3,
                1,
                2,
                3,
                1,
                2,
                3,
                1,
                2,
                3,
                1,
                2,
                3,
            ],
            "variable_name": [
                "STUDYID",
                "STUDYID",
                "STUDYID",
                "USUBJID",
                "USUBJID",
                "USUBJID",
                "AETERM",
                "AETERM",
                "AETERM",
                "STUDYID",
                "STUDYID",
                "STUDYID",
                "USUBJID",
                "USUBJID",
                "USUBJID",
                "AETERM",
                "AETERM",
                "AETERM",
            ],
            "variable_value": [
                "STUDY1",
                "STUDY1",
                "STUDY1",
                "SUBJ-001",
                "SUBJ-002",
                "SUBJ-003",
                "Headache",
                "Fever",
                "Nausea",
                "STUDY1",
                "STUDY1",
                "STUDY1",
                "SUBJ-004",
                "SUBJ-005",
                "SUBJ-006",
                "Dizziness",
                "Rash",
                "Fatigue",
            ],
            "variable_label": [
                "Study ID",
                "Study ID",
                "Study ID",
                "Subject ID",
                "Subject ID",
                "Subject ID",
                "AE Term",
                "AE Term",
                "AE Term",
                "Study ID",
                "Study ID",
                "Study ID",
                "Subject ID",
                "Subject ID",
                "Subject ID",
                "AE Term",
                "AE Term",
                "AE Term",
            ],
            "variable_size": [
                16,
                16,
                16,
                20,
                20,
                20,
                200,
                200,
                200,
                16,
                16,
                16,
                20,
                20,
                20,
                200,
                200,
                200,
            ],
            "variable_order_number": [
                1,
                1,
                1,
                2,
                2,
                2,
                9,
                9,
                9,
                1,
                1,
                1,
                2,
                2,
                2,
                9,
                9,
                9,
            ],
            "variable_data_type": ["text"] * 18,
            "variable_format": [
                "$16.",
                "$16.",
                "$16.",
                "$20.",
                "$20.",
                "$20.",
                "$200.",
                "$200.",
                "$200.",
                "$16.",
                "$16.",
                "$16.",
                "$20.",
                "$20.",
                "$20.",
                "$200.",
                "$200.",
                "$200.",
            ],
            "variable_value_length": [
                6,
                6,
                6,
                8,
                8,
                8,
                8,
                5,
                6,
                6,
                6,
                6,
                8,
                8,
                8,
                9,
                4,
                7,
            ],
            "source_filename": [
                "ae1.xpt",
                "ae1.xpt",
                "ae1.xpt",
                "ae1.xpt",
                "ae1.xpt",
                "ae1.xpt",
                "ae1.xpt",
                "ae1.xpt",
                "ae1.xpt",
                "ae2.xpt",
                "ae2.xpt",
                "ae2.xpt",
                "ae2.xpt",
                "ae2.xpt",
                "ae2.xpt",
                "ae2.xpt",
                "ae2.xpt",
                "ae2.xpt",
            ],
            "source_row_number": [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
            ],
        }
    )
    for col in result.data.columns:
        if col not in expected_data.columns:
            expected_data[col] = None

    assert len(result.data) == len(expected_data)
    key_columns = [
        "row_number",
        "variable_name",
        "variable_value",
        "source_filename",
        "source_row_number",
    ]
    for col in key_columns:
        pd.testing.assert_series_equal(
            result.data[col], expected_data[col], check_names=False
        )
