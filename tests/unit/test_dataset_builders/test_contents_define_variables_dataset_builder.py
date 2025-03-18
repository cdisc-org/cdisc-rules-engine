from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import numpy as np
from cdisc_rules_engine.dataset_builders.contents_define_variables_dataset_builder import (  # noqa: E501
    ContentsDefineVariablesDatasetBuilder,
)
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.models.dataset import PandasDataset


@pytest.mark.parametrize(
    "dataset_implementation, content, variables_metadata, expected",
    [
        (
            PandasDataset,
            {"VAR1": ["1A", "1B", "1C"], "VAR2": ["2A", "2B", "2C"]},
            {
                "define_variable_name": ["VAR1", "VAR3"],
                "define_variable_label": ["VAR1 Label", "VAR3 Label"],
                "define_variable_data_type": ["VAR1 Type", "VAR3 Type"],
                "define_variable_role": ["VAR1 ROLE", "VAR3 ROLE"],
            },
            {
                "row_number": [1, 2, 3, 1, 2, 3, np.nan],
                "variable_name": [
                    "VAR1",
                    "VAR1",
                    "VAR1",
                    "VAR2",
                    "VAR2",
                    "VAR2",
                    None,
                ],
                "variable_value": ["1A", "1B", "1C", "2A", "2B", "2C", None],
                "define_variable_name": [
                    "VAR1",
                    "VAR1",
                    "VAR1",
                    None,
                    None,
                    None,
                    "VAR3",
                ],
                "define_variable_label": [
                    "VAR1 Label",
                    "VAR1 Label",
                    "VAR1 Label",
                    None,
                    None,
                    None,
                    "VAR3 Label",
                ],
                "define_variable_data_type": [
                    "VAR1 Type",
                    "VAR1 Type",
                    "VAR1 Type",
                    None,
                    None,
                    None,
                    "VAR3 Type",
                ],
                "define_variable_role": [
                    "VAR1 ROLE",
                    "VAR1 ROLE",
                    "VAR1 ROLE",
                    None,
                    None,
                    None,
                    "VAR3 ROLE",
                ],
            },
        ),
    ],
)
@patch(
    "cdisc_rules_engine.dataset_builders.base_dataset_builder."
    + "BaseDatasetBuilder.get_define_xml_variables_metadata"
)
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
)
def test_contents_define_variables_dataset_builder(
    mock_get_dataset: MagicMock,
    mock_get_define_xml_variables_metadata: MagicMock,
    dataset_implementation,
    content,
    variables_metadata,
    expected,
):
    mock_get_dataset.return_value = dataset_implementation.from_dict(content)
    mock_get_define_xml_variables_metadata.return_value = pd.DataFrame.from_dict(
        variables_metadata
    ).to_records(index=False)
    result = ContentsDefineVariablesDatasetBuilder(
        rule=None,
        data_service=LocalDataService(MagicMock(), MagicMock(), MagicMock()),
        cache_service=None,
        rule_processor=None,
        data_processor=None,
        dataset_path=None,
        datasets=None,
        dataset_metadata=None,
        define_xml_path=None,
        standard="sdtmig",
        standard_version="3-4",
        standard_substandard=None,
        library_metadata=LibraryMetadataContainer(),
    ).build()
    expected_data = dataset_implementation.from_dict(expected)
    assert result.equals(expected_data)
