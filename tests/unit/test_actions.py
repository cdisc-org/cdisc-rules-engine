import pandas as pd
import numpy as np

from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.actions import COREActions
from cdisc_rules_engine.models.dataset_variable import DatasetVariable
from cdisc_rules_engine.enums.variable_roles import VariableRoles
import json
import pytest

from cdisc_rules_engine.utilities.utils import tag_source
from cdisc_rules_engine.constants.metadata_columns import (
    SOURCE_FILENAME,
    SOURCE_ROW_NUMBER,
)


def _create_test_dataframe_and_metadata(data_dict, domain="TX", filename="tx.xpt"):
    """Helper to create test DataFrame and metadata."""
    df = pd.DataFrame(data_dict)
    variable = DatasetVariable(PandasDataset(df))
    dataset_metadata = SDTMDatasetMetadata(
        first_record={"DOMAIN": domain}, filename=filename
    )
    return df, variable, dataset_metadata


def _create_test_action(rule, df, variable, dataset_metadata):
    """Helper to create COREActions instance."""
    return COREActions([], variable, dataset_metadata, rule)


def test_targeted_error_object_with_partial_missing_targets():
    dummy_rule = {
        "core_id": "MockRule",
        "conditions": {
            "all": [
                {
                    "name": "get_dataset",
                    "operator": "greater_than",
                    "value": {
                        "target": "TEST",
                        "comparator": 0,
                    },
                }
            ]
        },
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "TEST greater than 0",
                },
            }
        ],
        "output_variables": ["TEST", "MISSING"],
    }
    df = pd.DataFrame.from_dict({"TEST": [1, 2, 3, 4]})
    variable = DatasetVariable(df)
    action = COREActions([], variable, SDTMDatasetMetadata(name="TV"), dummy_rule)
    targets = set(dummy_rule["output_variables"])
    result = action.generate_targeted_error_object(targets, df, "TEST greater than 0")
    for error in result.errors:
        assert "MISSING" in error.value
        assert error.value["MISSING"] == "Not in dataset"


def test_targeted_error_object_with_dataset_sensitivity():
    dummy_rule = {
        "core_id": "MockRule",
        "sensitivity": "Dataset",
        "conditions": {
            "all": [
                {
                    "name": "get_dataset",
                    "operator": "greater_than",
                    "value": {
                        "target": "TEST",
                        "comparator": 0,
                    },
                }
            ]
        },
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "TEST greater than 0",
                },
            }
        ],
        "output_variables": ["TEST", "MISSING"],
    }
    df = pd.DataFrame.from_dict({"TEST": [1, 2, 3, 4]})
    variable = DatasetVariable(df)
    action = COREActions(
        [], variable, SDTMDatasetMetadata(first_record={"DOMAIN": "TV"}), dummy_rule
    )
    targets = set(dummy_rule["output_variables"])
    result = action.generate_targeted_error_object(targets, df, "TEST greater than 0")
    assert len(result.errors) == 1
    error = result.errors[0].to_representation()
    assert "row" not in error
    assert error["value"] == {"TEST": 1, "MISSING": "Not in dataset"}


def test_targeted_error_object_with_group_sensitivity():
    dummy_rule = {
        "core_id": "FB4607",
        "sensitivity": "Group",
        "grouping_variables": ["SETCD"],
        "conditions": {
            "all": [
                {
                    "name": "get_dataset",
                    "operator": "non_empty",
                    "value": {"target": "TXPARMCD"},
                }
            ]
        },
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {"message": "Missing required TXPARMCD value per set"},
            }
        ],
        "output_variables": ["SETCD", "TXPARMCD"],
    }

    # Create test data with 3 groups (SETCD), multiple records each
    df = pd.DataFrame.from_dict(
        {
            "TXPARMCD": [
                "PLANSUB",
                "PLANSUB",
                "PLANSUB",
                "OTHER",
                "OTHER",
                "OTHER",
                "OTHER",
                "VALUE",
                "VALUE",
            ],
            "SETCD": [
                "SET1",
                "SET1",
                "SET1",
                "SET2",
                "SET2",
                "SET2",
                "SET2",
                "SET3",
                "SET3",
            ],
            "USUBJID": ["001", "001", "001", "002", "002", "002", "002", "003", "003"],
        }
    )

    variable = DatasetVariable(PandasDataset(df))
    dataset_metadata = SDTMDatasetMetadata(
        first_record={"DOMAIN": "TX"}, filename="tx.xpt"
    )
    action = COREActions([], variable, dataset_metadata, dummy_rule)

    targets = set(dummy_rule["output_variables"])
    result = action.generate_targeted_error_object(
        targets, df, "Missing required TXPARMCD value per set"
    )

    # Should have exactly 3 errors (one per SETCD group)
    assert len(result.errors) == 3

    # Extract SETCD values from errors
    setcd_values = [
        error.to_representation()["value"]["SETCD"] for error in result.errors
    ]
    assert sorted(setcd_values) == ["SET1", "SET2", "SET3"]

    # Each error should have row information from the first record in its group
    for error in result.errors:
        error_repr = error.to_representation()
        assert "row" in error_repr
        assert "SETCD" in error_repr["value"]
        assert "TXPARMCD" in error_repr["value"]


def test_group_sensitivity_missing_grouping_variables():
    """Test that Group sensitivity requires Grouping_Variables."""
    dummy_rule = {
        "core_id": "FB4607",
        "sensitivity": "Group",
        "grouping_variables": [],
        "conditions": {"all": []},
        "actions": [{"name": "generate_dataset_error_objects"}],
        "output_variables": ["TXPARMCD"],
    }

    df, variable, dataset_metadata = _create_test_dataframe_and_metadata(
        {"TXPARMCD": ["VALUE"]}
    )
    action = _create_test_action(dummy_rule, df, variable, dataset_metadata)

    result = action.generate_targeted_error_object(set(), df, "Test message")

    assert len(result.errors) == 1
    assert "Group sensitivity requires Grouping_Variables" in result.message


def test_group_sensitivity_invalid_grouping_variables():
    """Test that Group sensitivity validates grouping variables exist in dataset."""
    dummy_rule = {
        "core_id": "FB4607",
        "sensitivity": "Group",
        "grouping_variables": ["INVALID_VAR"],
        "conditions": {"all": []},
        "actions": [{"name": "generate_dataset_error_objects"}],
        "output_variables": ["TXPARMCD"],
    }

    df, variable, dataset_metadata = _create_test_dataframe_and_metadata(
        {"TXPARMCD": ["VALUE"]}
    )
    action = _create_test_action(dummy_rule, df, variable, dataset_metadata)

    result = action.generate_targeted_error_object(set(), df, "Test message")

    assert len(result.errors) == 1
    assert "Grouping variables not found in dataset" in result.message


def test_group_sensitivity_multiple_grouping_variables():
    """Test Group sensitivity with multiple grouping variables."""
    dummy_rule = {
        "core_id": "FB4607",
        "sensitivity": "Group",
        "grouping_variables": ["SETCD", "USUBJID"],
        "conditions": {"all": []},
        "actions": [{"name": "generate_dataset_error_objects"}],
        "output_variables": ["TXPARMCD"],
    }

    df, variable, dataset_metadata = _create_test_dataframe_and_metadata(
        {
            "TXPARMCD": ["VALUE1", "VALUE2", "VALUE3"],
            "SETCD": ["SET1", "SET1", "SET2"],
            "USUBJID": ["001", "002", "001"],
        }
    )
    action = _create_test_action(dummy_rule, df, variable, dataset_metadata)

    result = action.generate_targeted_error_object(set(), df, "Test message")

    # Should have 3 groups: (SET1,001), (SET1,002), (SET2,001)
    assert len(result.errors) == 3

    # Check that each error has both grouping variables
    for error in result.errors:
        error_repr = error.to_representation()
        assert "SETCD" in error_repr["value"]
        assert "USUBJID" in error_repr["value"]


def test_empty_sequential():
    dummy_rule = {
        "core_id": "MockRule",
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "TEST greater than 2",
                },
            }
        ],
        "output_variables": ["TV"],
    }
    df = pd.DataFrame.from_dict(
        {"TVSEQ": [2, 4, 6, None, "", 8], "TV": [1, 3, 5, 7, 9, "8"]}
    )
    variable = DatasetVariable(df)
    dataset_metadata = SDTMDatasetMetadata(first_record={"DOMAIN": "TV"}, filename="tv")
    action = COREActions(
        [],
        variable,
        dataset_metadata,
        dummy_rule,
    )
    targets = set(dummy_rule["output_variables"])
    result = action.generate_targeted_error_object(
        targets,
        tag_source(PandasDataset(df), dataset_metadata).data,
        "TVSEQ greater than 2",
    )
    assert [err.to_representation() for err in result.errors] == [
        {"value": {"TV": 1}, "dataset": "tv", "row": 1, "SEQ": 2},
        {"value": {"TV": 3}, "dataset": "tv", "row": 2, "SEQ": 4},
        {"value": {"TV": 5}, "dataset": "tv", "row": 3, "SEQ": 6},
        {"value": {"TV": 7}, "dataset": "tv", "row": 4},
        {"value": {"TV": 9}, "dataset": "tv", "row": 5},
        {"value": {"TV": "8"}, "dataset": "tv", "row": 6, "SEQ": 8},
    ]


@pytest.mark.parametrize(
    "data",
    [(set([1, 2, 3])), (VariableRoles.IDENTIFIER)],
)
def test_json_serializable_value(data):
    dummy_rule = {
        "core_id": "MockRule",
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "TEST greater than 2",
                },
            }
        ],
        "output_variables": ["TV", "NEWDATA"],
    }
    df = pd.DataFrame.from_dict(
        {
            "TVSEQ": [2, 4, 6, None, "", 8],
            "TV": [1, 3, 5, 7, 9, "8"],
            "NEWDATA": [data, data, data, data, data, data],
        }
    )
    variable = DatasetVariable(df)
    action = COREActions(
        [], variable, SDTMDatasetMetadata(first_record={"DOMAIN": "TV"}), dummy_rule
    )
    targets = set(dummy_rule["output_variables"])
    result = action.generate_targeted_error_object(targets, df, "TVSEQ greater than 2")
    # Ensure json dumps does not throw an error
    json.dumps(result.to_representation())


def test_extract_comparison_metadata():
    """Test extraction of comparison metadata from output_variables."""
    dummy_rule = {
        "core_id": "ComparisonTest",
        "output_variables": [
            "$VAR1",
            {"compared": ["$VAR2", "$VAR3", "$VAR4"]},
            "$VAR5",
        ],
    }
    df = pd.DataFrame({"VAR1": [1], "VAR2": [2], "VAR3": [3], "VAR4": [4], "VAR5": [5]})
    variable = DatasetVariable(df)
    dataset_metadata = SDTMDatasetMetadata(
        first_record={"DOMAIN": "AE"}, filename="ae.xpt"
    )
    action = COREActions([], variable, dataset_metadata, dummy_rule)

    compare_groups = action._extract_comparison_metadata(dummy_rule)
    assert compare_groups == [["$VAR2", "$VAR3", "$VAR4"]]
    assert dummy_rule["output_variables"] == [
        "$VAR1",
        "$VAR2",
        "$VAR3",
        "$VAR4",
        "$VAR5",
    ]


def test_extract_comparison_metadata_multiple_groups():
    """Test extraction with multiple compared blocks."""
    dummy_rule = {
        "core_id": "ComparisonTest",
        "output_variables": [
            "$VAR1",
            {"compared": ["$VAR2", "$VAR3"]},
            {"compared": ["$VAR4", "$VAR5", "$VAR6"]},
        ],
    }
    df = pd.DataFrame(
        {"VAR1": [1], "VAR2": [2], "VAR3": [3], "VAR4": [4], "VAR5": [5], "VAR6": [6]}
    )
    variable = DatasetVariable(df)
    dataset_metadata = SDTMDatasetMetadata(
        first_record={"DOMAIN": "AE"}, filename="ae.xpt"
    )
    action = COREActions([], variable, dataset_metadata, dummy_rule)

    compare_groups = action._extract_comparison_metadata(dummy_rule)
    assert compare_groups == [["$VAR2", "$VAR3"], ["$VAR4", "$VAR5", "$VAR6"]]


def test_extract_comparison_metadata_caching():
    """Test that comparison metadata is cached in rule dict."""
    dummy_rule = {
        "core_id": "ComparisonTest",
        "output_variables": [{"compared": ["$VAR1", "$VAR2"]}],
    }
    df = pd.DataFrame({"VAR1": [1], "VAR2": [2]})
    variable = DatasetVariable(df)
    dataset_metadata = SDTMDatasetMetadata(
        first_record={"DOMAIN": "AE"}, filename="ae.xpt"
    )
    action = COREActions([], variable, dataset_metadata, dummy_rule)

    compare_groups1 = action._extract_comparison_metadata(dummy_rule)
    compare_groups2 = action._extract_comparison_metadata(dummy_rule)

    assert compare_groups1 == compare_groups2 == [["$VAR1", "$VAR2"]]
    assert "_cached_compare_groups" in dummy_rule


def test_extract_comparison_metadata_no_compared():
    """Test that None is returned when no compared blocks exist."""
    dummy_rule = {
        "core_id": "NoComparisonTest",
        "output_variables": ["$VAR1", "$VAR2"],
    }
    df = pd.DataFrame({"VAR1": [1], "VAR2": [2]})
    variable = DatasetVariable(df)
    dataset_metadata = SDTMDatasetMetadata(
        first_record={"DOMAIN": "AE"}, filename="ae.xpt"
    )
    action = COREActions([], variable, dataset_metadata, dummy_rule)

    compare_groups = action._extract_comparison_metadata(dummy_rule)
    assert compare_groups is None


def test_generate_targeted_error_object_with_compare_groups():
    """Test that compare_groups are included in ValidationErrorContainer."""
    dummy_rule = {
        "core_id": "ComparisonTest",
        "actions": [
            {
                "name": "generate_targeted_error_objects",
                "params": {"message": "Comparison test"},
            }
        ],
        "output_variables": [{"compared": ["$VAR1", "$VAR2"]}],
    }
    df = pd.DataFrame(
        {
            "VAR1": [1, 2],
            "VAR2": [2, 3],
            "USUBJID": ["SUBJ-001", "SUBJ-002"],
        }
    )
    df[SOURCE_FILENAME] = "test.xpt"
    df[SOURCE_ROW_NUMBER] = [1, 2]
    variable = DatasetVariable(df)
    dataset_metadata = SDTMDatasetMetadata(
        first_record={"DOMAIN": "AE"}, filename="test.xpt"
    )
    action = COREActions([], variable, dataset_metadata, dummy_rule)

    result = action.generate_targeted_error_object(
        {"VAR1", "VAR2"}, df, "Comparison test"
    )
    assert result.compare_groups == [["$VAR1", "$VAR2"]]
    representation = result.to_representation()
    assert "compare_groups" in representation
    assert representation["compare_groups"] == [["$VAR1", "$VAR2"]]


def test_nan_handling_in_error_object():
    dummy_rule = {
        "core_id": "NaNTest",
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Testing NaN handling",
                },
            }
        ],
        "output_variables": ["TEST", "NAN_VAL", "NAN_LIST"],
    }
    df = pd.DataFrame(
        {
            "TEST": ["Value1", "Value2", "Value3", "Value4"],
            "NAN_VAL": [1.0, np.nan, 3.0, np.nan],
            "NAN_LIST": [
                [1.0, 2.0, 3.0],
                [4.0, np.nan, 6.0],
                ["a", "b", "c"],
                [7.0, 8.0, np.nan],
            ],
            "USUBJID": ["SUBJ-001", "SUBJ-002", "SUBJ-003", "SUBJ-004"],
            "TVSEQ": [1, 2, 3, 4],
        }
    )
    df[SOURCE_FILENAME] = "test.xpt"
    df[SOURCE_ROW_NUMBER] = [1, 2, 3, 4]

    expected_nan_vals = [1.0, None, 3.0, None]
    expected_nan_lists = [
        [1.0, 2.0, 3.0],
        [4.0, None, 6.0],
        ["a", "b", "c"],
        [7.0, 8.0, None],
    ]
    variable = DatasetVariable(df)
    dataset_metadata = SDTMDatasetMetadata(
        first_record={"DOMAIN": "TV"}, filename="test.xpt"
    )
    action = COREActions([], variable, dataset_metadata, dummy_rule)

    for i in range(len(df)):
        row = df.iloc[i]
        result = action._create_error_object(row, df)
        expected_val = expected_nan_vals[i]
        expected_list = expected_nan_lists[i]
        assert (
            result.value["NAN_VAL"] == expected_val
        ), f"Row {i}: NAN_VAL does not match expected. Got {result.value['NAN_VAL']}, expected {expected_val}"
        assert (
            result.value["NAN_LIST"] == expected_list
        ), f"Row {i}: NAN_LIST does not match expected. Got {result.value['NAN_LIST']}, expected {expected_list}"

    all_results = action.generate_targeted_error_object(
        set(dummy_rule["output_variables"]), df, "Testing NaN handling"
    )
    json_output = json.dumps(all_results.to_representation())
    assert (
        '"NAN_VAL": null' in json_output
    ), "Missing null value for NAN_VAL in JSON output"
    assert (
        "[4.0, null, 6.0]" in json_output
    ), "Missing list with individual null element [4.0, null, 6.0]"
    assert (
        "[7.0, 8.0, null]" in json_output
    ), "Missing list with individual null element [7.0, 8.0, null]"
