from cdisc_rules_engine.models.rule_conditions.single_condition import SingleCondition


def test_to_dict():
    """
    Unit test for to_dict() method.
    """
    condition_dict = {
        "name": "get_dataset",
        "operator": "suffix_not_equal_to",
        "value": {"target": "$dataset_name", "comparator": "RDOMAIN", "suffix": 2},
    }
    condition = SingleCondition(condition_dict)
    assert condition.to_dict() == condition_dict


def test_values():
    """
    Unit test for values() method.
    """
    condition_dict = {
        "name": "get_dataset",
        "operator": "suffix_not_equal_to",
        "value": {"target": "$dataset_name", "comparator": "RDOMAIN", "suffix": 2},
    }
    condition = SingleCondition(condition_dict)
    assert condition.values() == [condition_dict]
