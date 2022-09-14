from cdisc_rules_engine.models.rule_conditions.condition_composite import (
    ConditionComposite,
)
from cdisc_rules_engine.models.rule_conditions.single_condition import SingleCondition


def test_add_conditions():
    """
    Unit test for ConditionComposite.add_conditions method.
    """
    composite = ConditionComposite()
    single_condition = SingleCondition(
        {
            "name": "get_dataset",
            "operator": "suffix_not_equal_to",
            "value": {
                "target": "$dataset_name",
                "comparator": "RDOMAIN",
                "suffix": 2,
            },
        }
    )
    single_condition_1 = SingleCondition(
        {
            "name": "get_dataset",
            "operator": "equal_to",
            "value": {
                "target": "AESTDY",
                "comparator": "TEST",
            },
        }
    )
    composite.add_conditions("all", [single_condition, single_condition_1])

    assert composite.to_dict() == {
        "all": [
            single_condition.to_dict(),
            single_condition_1.to_dict(),
        ]
    }


def test_values():
    """
    Unit test for ConditionComposite.values method.
    """
    composite = ConditionComposite()
    single_condition = SingleCondition(
        {
            "name": "get_dataset",
            "operator": "suffix_not_equal_to",
            "value": {
                "target": "$dataset_name",
                "comparator": "RDOMAIN",
                "suffix": 2,
            },
        }
    )
    single_condition_1 = SingleCondition(
        {
            "name": "get_dataset",
            "operator": "equal_to",
            "value": {
                "target": "AESTDY",
                "comparator": "TEST",
            },
        }
    )
    composite.add_conditions("all", [single_condition, single_condition_1])

    assert composite.values() == [
        single_condition.to_dict(),
        single_condition_1.to_dict(),
    ]


def test_items():
    """
    Unit test for ConditionComposite.items method.
    """
    composite = ConditionComposite()
    single_condition = SingleCondition(
        {
            "name": "get_dataset",
            "operator": "suffix_not_equal_to",
            "value": {
                "target": "$dataset_name",
                "comparator": "RDOMAIN",
                "suffix": 2,
            },
        }
    )
    single_condition_1 = SingleCondition(
        {
            "name": "get_dataset",
            "operator": "equal_to",
            "value": {
                "target": "AESTDY",
                "comparator": "TEST",
            },
        }
    )
    composite.add_conditions("all", [single_condition, single_condition_1])

    assert composite.items() == [
        (
            "all",
            [
                single_condition.to_dict(),
                single_condition_1.to_dict(),
            ],
        )
    ]


def test_add_variable_conditions():
    """
    Unit test for ConditionComposite.add_variable_condtions method.
    Tests that conditions that need to be duplicated are.
    """
    composite = ConditionComposite()
    single_condition = SingleCondition(
        {
            "name": "get_dataset",
            "operator": "suffix_not_equal_to",
            "value": {
                "target": "$dataset_name",
                "comparator": "RDOMAIN",
                "suffix": 2,
            },
        }
    )
    single_condition_1 = SingleCondition(
        {
            "name": "get_dataset",
            "operator": "equal_to",
            "value": {
                "target": "AESTDY",
                "comparator": "TEST",
            },
            "variables": "all",
        }
    )
    composite.add_conditions("all", [single_condition, single_condition_1])
    targets = ["AESTDY", "AESCAT", "AEWWWR"]
    composite = composite.add_variable_conditions(targets)
    items = composite.items()
    check = items[0]
    assert len(check[1]) == 4
    assert check[0] == "all"
    assert check[1][0] == single_condition.to_dict()
    for target in targets:
        # Assert there is one condition for each target in the targets list
        assert (
            len([cond for cond in check[1][1:] if cond["value"]["target"] == target])
            == 1
        )


def test_add_variable_conditions_nested():
    """
    Unit test for ConditionComposite.add_variable_condtions method.
    Tests that conditions that need to be duplicated are.
    """
    composite = ConditionComposite()
    single_condition = SingleCondition(
        {
            "name": "get_dataset",
            "operator": "suffix_not_equal_to",
            "value": {
                "target": "$dataset_name",
                "comparator": "RDOMAIN",
                "suffix": 2,
            },
        }
    )
    nested_composite = ConditionComposite()
    single_condition_1 = SingleCondition(
        {
            "name": "get_dataset",
            "operator": "equal_to",
            "value": {
                "target": "AESTDY",
                "comparator": "TEST",
            },
            "variables": "all",
        }
    )
    nested_composite.add_conditions("any", [single_condition_1])
    composite.add_conditions("all", [single_condition, nested_composite])
    targets = ["AESTDY", "AESCAT", "AEWWWR"]
    composite = composite.add_variable_conditions(targets)
    items = composite.items()
    check = items[0]
    assert check[0] == "all"
    assert len(check[1]) == 2
    assert check[1][0] == single_condition.to_dict()
    nested_condition_dict = check[1][1]
    assert "any" in nested_condition_dict
    assert len(nested_condition_dict["any"]) == len(targets)
    for target in targets:
        # Assert there is one condition for each target in the targets list
        assert (
            len(
                [
                    cond
                    for cond in nested_condition_dict["any"]
                    if cond["value"]["target"] == target
                ]
            )
            == 1
        )
