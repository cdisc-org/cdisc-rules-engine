from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
import pytest
from unittest.mock import MagicMock, patch
from cdisc_rules_engine.dataset_builders.contents_define_vlm_dataset_builder import (  # noqa: E501
    ContentsDefineVLMDatasetBuilder,
)
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.models.dataset import PandasDataset, DaskDataset


@pytest.mark.parametrize(
    "dataset_implementation, content, vlm_metadata, expected",
    [
        (
            PandasDataset,
            {"VAR1": ["1A", "1B", "1C"], "VAR2": ["2A", "2B", "2C"]},
            {
                "define_variable_name": ["VAR1", "VAR3"],
                "define_vlm_name": ["VAR1B VLM Name", "VAR3C VLM Name"],
                "define_vlm_label": ["VAR1B Label", "VAR3C Label"],
                "define_vlm_data_type": ["text", "text"],
                "define_vlm_role": ["VAR1B ROLE", "VAR3C ROLE"],
                "define_vlm_length": [1, 2],
                "filter": [
                    lambda row: row["VAR2"] == "2B",
                    lambda row: row["VAR2"] == "2C",
                ],
                "define_vlm_mandatory": ["Yes", "No"],
            },
            {
                "row_number": [2],
                "variable_name": ["VAR1"],
                "variable_value": ["1B"],
                "variable_value_length": [2],
                "define_variable_name": [
                    "VAR1",
                ],
                "define_vlm_name": [
                    "VAR1B VLM Name",
                ],
                "define_vlm_label": [
                    "VAR1B Label",
                ],
                "define_vlm_data_type": [
                    "text",
                ],
                "define_vlm_role": [
                    "VAR1B ROLE",
                ],
                "define_vlm_length": [1],
                "define_vlm_mandatory": ["Yes"],
            },
        ),
        (
            DaskDataset,
            {"VAR1": ["1A", "1B", "1C"], "VAR2": ["2A", "2B", "2C"]},
            {
                "define_variable_name": ["VAR1", "VAR3"],
                "define_vlm_name": ["VAR1B VLM Name", "VAR3C VLM Name"],
                "define_vlm_label": ["VAR1B Label", "VAR3C Label"],
                "define_vlm_data_type": ["text", "text"],
                "define_vlm_role": ["VAR1B ROLE", "VAR3C ROLE"],
                "define_vlm_length": [1, 2],
                "filter": [
                    lambda row: row["VAR2"] == "2B",
                    lambda row: row["VAR2"] == "2C",
                ],
                "define_vlm_mandatory": ["Yes", "No"],
            },
            {
                "row_number": [2],
                "variable_name": ["VAR1"],
                "variable_value": ["1B"],
                "variable_value_length": [2],
                "define_variable_name": [
                    "VAR1",
                ],
                "define_vlm_name": [
                    "VAR1B VLM Name",
                ],
                "define_vlm_label": [
                    "VAR1B Label",
                ],
                "define_vlm_data_type": [
                    "text",
                ],
                "define_vlm_role": [
                    "VAR1B ROLE",
                ],
                "define_vlm_length": [1],
                "define_vlm_mandatory": ["Yes"],
            },
        ),
    ],
)
@patch(
    "cdisc_rules_engine.dataset_builders.base_dataset_builder."
    + "BaseDatasetBuilder.get_define_xml_value_level_metadata"
)
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
)
def test_contents_define_vlm_dataset_builder(
    mock_get_dataset: MagicMock,
    mock_get_define_xml_value_level_metadata: MagicMock,
    dataset_implementation,
    content,
    vlm_metadata,
    expected,
):
    mock_get_dataset.return_value = dataset_implementation.from_dict(content)
    mock_get_define_xml_value_level_metadata.return_value = vlm_metadata
    result = ContentsDefineVLMDatasetBuilder(
        rule=None,
        data_service=LocalDataService(
            MagicMock(),
            MagicMock(),
            MagicMock(),
            dataset_implementation=dataset_implementation,
        ),
        cache_service=None,
        rule_processor=None,
        data_processor=None,
        dataset_path=None,
        datasets=None,
        dataset_metadata=None,
        define_xml_path=None,
        standard="sdtmig",
        standard_version="3-4",
        library_metadata=LibraryMetadataContainer(),
    ).build()
    expected_df = dataset_implementation.from_dict(expected)
    if dataset_implementation == PandasDataset:
        expected_df.data.sort_index(axis=1, inplace=True)
        result.data.sort_index(axis=1, inplace=True)
        assert result.equals(expected_df)
    else:
        columns_to_check = [
            "row_number",
            "variable_name",
            "variable_value",
            "variable_value_length",
            "define_variable_name",
            "define_vlm_name",
            "define_vlm_label",
            "define_vlm_data_type",
            "define_vlm_role",
            "define_vlm_mandatory",
        ]
        for column in columns_to_check:
            assert (
                result.data[column].compute() == expected_df.data[column].compute()
            ).all()
