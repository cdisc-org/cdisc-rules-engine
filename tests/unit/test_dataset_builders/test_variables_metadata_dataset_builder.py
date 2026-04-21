from unittest.mock import MagicMock, patch
import pandas as pd
from cdisc_rules_engine.dataset_builders.variables_metadata_dataset_builder import (
    VariablesMetadataDatasetBuilder,
)
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.models.rule_conditions import ConditionCompositeFactory


def create_data_service_mock(mock_get_vars, mock_get_dataset):
    """Helper to create data_service mock with all required attributes."""
    svc = MagicMock(spec=LocalDataService)
    svc.get_variables_metadata = mock_get_vars
    svc.get_dataset = mock_get_dataset
    svc.dataset_implementation = PandasDataset
    return svc


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_variables_metadata"
)
def test_variables_metadata_without_max_size(mock_get_vars, mock_get_ds):
    """Test that variable_max_size is NOT computed when not required."""
    mock_get_vars.return_value = PandasDataset(
        pd.DataFrame(
            {
                "variable_name": ["STUDYID", "USUBJID", "AETERM"],
                "variable_label": ["Study ID", "Subject ID", "AE Term"],
                "variable_size": [16, 20, 200],
                "variable_order_number": [1, 2, 3],
                "variable_data_type": ["Char", "Char", "Char"],
                "variable_format": ["", "", ""],
            }
        )
    )

    conditions_dict = {"all": [{"name": "get_dataset", "operator": "non_empty"}]}

    rule = {
        "operations": None,
        "conditions": ConditionCompositeFactory.get_condition_composite(
            conditions_dict
        ),
        "output_variables": ["variable_name", "variable_size"],
    }

    builder = VariablesMetadataDatasetBuilder(
        rule=rule,
        data_service=create_data_service_mock(mock_get_vars, mock_get_ds),
        cache_service=InMemoryCacheService(),
        rule_processor=MagicMock(),
        data_processor=None,
        dataset_path="/test/ae.xpt",
        datasets=[],
        dataset_metadata=MagicMock(),
        define_xml_path=None,
        standard="sdtmig",
        standard_version="3-4",
        standard_substandard=None,
    )

    result = builder.build()

    mock_get_ds.assert_not_called()
    assert "variable_max_size" not in result.data.columns
    assert "variable_name" in result.data.columns
    assert len(result.data) == 3


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_variables_metadata"
)
def test_variables_metadata_with_max_size_in_operations(mock_get_vars, mock_get_ds):
    """Test variable_max_size IS computed when in operations."""
    mock_get_vars.return_value = PandasDataset(
        pd.DataFrame(
            {
                "variable_name": ["STUDYID", "USUBJID"],
                "variable_label": ["Study ID", "Subject ID"],
                "variable_size": [16, 20],
                "variable_order_number": [1, 2],
                "variable_data_type": ["Char", "Char"],
                "variable_format": ["", ""],
            }
        )
    )

    mock_get_ds.return_value = PandasDataset(
        pd.DataFrame(
            {
                "STUDYID": ["STUDY001", "STUDY002"],
                "USUBJID": ["SUBJ-001", "SUBJ-002"],
            }
        )
    )

    conditions_dict = {"all": [{"name": "get_dataset", "operator": "non_empty"}]}

    rule = {
        "operations": [{"operator": "variable_max_size"}],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            conditions_dict
        ),
        "output_variables": ["variable_name", "variable_max_size"],
    }

    builder = VariablesMetadataDatasetBuilder(
        rule=rule,
        data_service=create_data_service_mock(mock_get_vars, mock_get_ds),
        cache_service=InMemoryCacheService(),
        rule_processor=MagicMock(),
        data_processor=None,
        dataset_path="/test/ae.xpt",
        datasets=[],
        dataset_metadata=MagicMock(),
        define_xml_path=None,
        standard="sdtmig",
        standard_version="3-4",
        standard_substandard=None,
    )

    result = builder.build()

    mock_get_ds.assert_called_once()
    assert "variable_max_size" in result.data.columns
    assert (
        result.data[result.data["variable_name"] == "STUDYID"][
            "variable_max_size"
        ].values[0]
        == 8
    )
    assert (
        result.data[result.data["variable_name"] == "USUBJID"][
            "variable_max_size"
        ].values[0]
        == 8
    )


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_variables_metadata"
)
def test_variables_metadata_with_max_size_in_output_variables(
    mock_get_vars, mock_get_ds
):
    """Test variable_max_size IS computed when in output_variables."""
    mock_get_vars.return_value = PandasDataset(
        pd.DataFrame(
            {
                "variable_name": ["STUDYID"],
                "variable_label": ["Study ID"],
                "variable_size": [16],
                "variable_order_number": [1],
                "variable_data_type": ["Char"],
                "variable_format": [""],
            }
        )
    )

    mock_get_ds.return_value = PandasDataset(pd.DataFrame({"STUDYID": ["STUDY001"]}))

    conditions_dict = {"all": [{"name": "get_dataset", "operator": "non_empty"}]}

    rule = {
        "operations": None,
        "conditions": ConditionCompositeFactory.get_condition_composite(
            conditions_dict
        ),
        "output_variables": ["variable_max_size"],
    }

    builder = VariablesMetadataDatasetBuilder(
        rule=rule,
        data_service=create_data_service_mock(mock_get_vars, mock_get_ds),
        cache_service=InMemoryCacheService(),
        rule_processor=MagicMock(),
        data_processor=None,
        dataset_path="/test/ae.xpt",
        datasets=[],
        dataset_metadata=MagicMock(),
        define_xml_path=None,
        standard="sdtmig",
        standard_version="3-4",
        standard_substandard=None,
    )

    result = builder.build()

    mock_get_ds.assert_called_once()
    assert "variable_max_size" in result.data.columns


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_variables_metadata"
)
def test_variables_metadata_with_max_size_in_conditions_dict(
    mock_get_vars, mock_get_ds
):
    """Test variable_max_size IS computed when in conditions (dict)."""
    mock_get_vars.return_value = PandasDataset(
        pd.DataFrame(
            {
                "variable_name": ["STUDYID"],
                "variable_label": ["Study ID"],
                "variable_size": [16],
                "variable_order_number": [1],
                "variable_data_type": ["Char"],
                "variable_format": [""],
            }
        )
    )

    mock_get_ds.return_value = PandasDataset(pd.DataFrame({"STUDYID": ["STUDY001"]}))

    conditions_dict = {
        "all": [
            {
                "name": "get_dataset",
                "operator": "equal_to",
                "value": {"target": "variable_size", "comparator": "variable_max_size"},
            }
        ]
    }

    rule = {
        "operations": None,
        "conditions": ConditionCompositeFactory.get_condition_composite(
            conditions_dict
        ),
        "output_variables": None,
    }

    builder = VariablesMetadataDatasetBuilder(
        rule=rule,
        data_service=create_data_service_mock(mock_get_vars, mock_get_ds),
        cache_service=InMemoryCacheService(),
        rule_processor=MagicMock(),
        data_processor=None,
        dataset_path="/test/ae.xpt",
        datasets=[],
        dataset_metadata=MagicMock(),
        define_xml_path=None,
        standard="sdtmig",
        standard_version="3-4",
        standard_substandard=None,
    )

    result = builder.build()

    mock_get_ds.assert_called_once()
    assert "variable_max_size" in result.data.columns


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_variables_metadata"
)
def test_variables_metadata_handles_nulls(mock_get_vars, mock_get_ds):
    """Test variable_max_size handles null values correctly."""
    mock_get_vars.return_value = PandasDataset(
        pd.DataFrame(
            {
                "variable_name": ["STUDYID", "AETERM"],
                "variable_label": ["Study ID", "AE Term"],
                "variable_size": [16, 200],
                "variable_order_number": [1, 2],
                "variable_data_type": ["Char", "Char"],
                "variable_format": ["", ""],
            }
        )
    )

    mock_get_ds.return_value = PandasDataset(
        pd.DataFrame(
            {
                "STUDYID": ["STUDY001", None, "STUDY002"],
                "AETERM": [None, None, "Hea"],
            }
        )
    )

    conditions_dict = {"all": [{"name": "get_dataset", "operator": "non_empty"}]}

    rule = {
        "operations": [{"operator": "variable_max_size"}],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            conditions_dict
        ),
        "output_variables": ["variable_max_size"],
    }

    builder = VariablesMetadataDatasetBuilder(
        rule=rule,
        data_service=create_data_service_mock(mock_get_vars, mock_get_ds),
        cache_service=InMemoryCacheService(),
        rule_processor=MagicMock(),
        data_processor=None,
        dataset_path="/test/ae.xpt",
        datasets=[],
        dataset_metadata=MagicMock(),
        define_xml_path=None,
        standard="sdtmig",
        standard_version="3-4",
        standard_substandard=None,
    )

    result = builder.build()

    assert (
        result.data[result.data["variable_name"] == "STUDYID"][
            "variable_max_size"
        ].values[0]
        == 8
    )
    assert (
        result.data[result.data["variable_name"] == "AETERM"][
            "variable_max_size"
        ].values[0]
        == 3
    )


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_variables_metadata"
)
def test_variables_metadata_handles_missing_columns(mock_get_vars, mock_get_ds):
    """Test variable_max_size handles variables not in dataset."""
    mock_get_vars.return_value = PandasDataset(
        pd.DataFrame(
            {
                "variable_name": ["STUDYID", "MISSINGVAR"],
                "variable_label": ["Study ID", "Missing"],
                "variable_size": [16, 20],
                "variable_order_number": [1, 2],
                "variable_data_type": ["Char", "Char"],
                "variable_format": ["", ""],
            }
        )
    )

    mock_get_ds.return_value = PandasDataset(pd.DataFrame({"STUDYID": ["STUDY001"]}))

    conditions_dict = {"all": [{"name": "get_dataset", "operator": "non_empty"}]}

    rule = {
        "operations": [{"operator": "variable_max_size"}],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            conditions_dict
        ),
        "output_variables": ["variable_max_size"],
    }

    builder = VariablesMetadataDatasetBuilder(
        rule=rule,
        data_service=create_data_service_mock(mock_get_vars, mock_get_ds),
        cache_service=InMemoryCacheService(),
        rule_processor=MagicMock(),
        data_processor=None,
        dataset_path="/test/ae.xpt",
        datasets=[],
        dataset_metadata=MagicMock(),
        define_xml_path=None,
        standard="sdtmig",
        standard_version="3-4",
        standard_substandard=None,
    )

    result = builder.build()

    assert (
        result.data[result.data["variable_name"] == "STUDYID"][
            "variable_max_size"
        ].values[0]
        == 8
    )
    assert (
        result.data[result.data["variable_name"] == "MISSINGVAR"][
            "variable_max_size"
        ].values[0]
        == 0
    )
