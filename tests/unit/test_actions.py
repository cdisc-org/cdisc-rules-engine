import pandas as pd

from cdisc_rules_engine.models.actions import COREActions
from cdisc_rules_engine.models.dataset_variable import DatasetVariable


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
    action = COREActions([], variable, "TV", dummy_rule)
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
    action = COREActions([], variable, "TV", dummy_rule)
    targets = set(dummy_rule["output_variables"])
    result = action.generate_targeted_error_object(targets, df, "TEST greater than 0")
    assert len(result.errors) == 1
    error = result.errors[0].to_representation()
    assert "row" not in error
    assert error["value"] == {"TEST": 1, "MISSING": "Not in dataset"}


def test_empty_sequential_not_raise_errors():
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
    action = COREActions([], variable, "TV", dummy_rule)
    targets = set(dummy_rule["output_variables"])
    result = action.generate_targeted_error_object(targets, df, "TVSEQ greater than 2")
    assert result.errors[3]._sequence is None
    assert result.errors[4]._sequence is None
    assert len(result.errors) == 6
