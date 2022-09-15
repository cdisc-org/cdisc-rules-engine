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


def test_duplicate():
    condition_dict = {
        "name": "get_dataset",
        "operator": "invalid_date",
        "value": {"comparator": True},
    }
    condition = SingleCondition(condition_dict)
    targets = ["AECDAT", "AEBDAT", "AELDAT"]
    duplicates = condition.duplicate(targets)
    assert len(duplicates) == len(targets)
    for duplicate in duplicates:
        new_condition = duplicate.values()[0]
        assert new_condition["name"] == condition_dict["name"]
        assert new_condition["operator"] == condition_dict["operator"]
        new_value = new_condition["value"]
        assert new_value["target"] in targets
        assert new_value["comparator"] == condition_dict["value"]["comparator"]
        assert "variables" not in new_condition
