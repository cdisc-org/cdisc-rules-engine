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
