from cdisc_rules_engine.models.rule_conditions.allowed_conditions_keys import (
    AllowedConditionsKeys,
)
from cdisc_rules_engine.models.rule_conditions.condition_composite import (
    ConditionComposite,
)
from cdisc_rules_engine.models.rule_conditions.not_condition_composite import (
    NotConditionComposite,
)
from cdisc_rules_engine.models.rule_conditions.single_condition import SingleCondition


def test_to_dict():
    """
    Unit test for NotConditionComposite.to_dict() method.
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

    not_composite = NotConditionComposite(
        key=AllowedConditionsKeys.NOT.value, condition_composite=composite
    )
    assert not_composite.to_dict() == {
        AllowedConditionsKeys.NOT.value: composite.to_dict()
    }


def test_values():
    """
    Unit test for NotConditionComposite.values() method.
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

    not_composite = NotConditionComposite(
        key=AllowedConditionsKeys.NOT.value, condition_composite=composite
    )
    assert not_composite.values() == [
        single_condition.to_dict(),
        single_condition_1.to_dict(),
    ]


def test_items():
    """
    Unit test for NotConditionComposite.items() method.
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

    not_composite = NotConditionComposite(
        key=AllowedConditionsKeys.NOT.value, condition_composite=composite
    )
    # "not" is not expected in items() result
    assert not_composite.items() == [
        (
            "all",
            [
                single_condition.to_dict(),
                single_condition_1.to_dict(),
            ],
        )
    ]
