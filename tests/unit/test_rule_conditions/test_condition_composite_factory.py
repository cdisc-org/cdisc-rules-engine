import pytest

from cdisc_rules_engine.exceptions.custom_exceptions import RuleFormatError
from cdisc_rules_engine.models.rule_conditions import ConditionCompositeFactory


def test_get_condition_composite():
    """
    The test ensures that the composite produced
    by the factory is the same as the original rule.
    """
    dataset_name_condition: dict = {"value": {"target": "dataset_name"}}
    dataset_label_condition: dict = {"value": {"target": "dataset_label"}}
    dataset_size_condition: dict = {"value": {"target": "dataset_size"}}
    dataset_location_condition: dict = {"value": {"target": "dataset_location"}}

    rule: dict = {
        "conditions": {
            "not": {
                "all": [
                    {
                        "any": [
                            dataset_name_condition,
                            dataset_label_condition,
                            {"all": [dataset_size_condition]},
                        ]
                    },
                    dataset_location_condition,
                ]
            },
        }
    }
    # factory produces composite which is not obliged to be the same as original input.
    composite = ConditionCompositeFactory.get_condition_composite(rule["conditions"])
    # check JSON serialization
    not_composite_expected = {"all": [rule["conditions"]]}
    assert composite.to_dict() == not_composite_expected


@pytest.mark.parametrize(
    "rule",
    [
        {"conditions": {"value": {"target": "dataset_location"}}},
        {
            "conditions": {
                "blablabla": [
                    {"value": {"target": "dataset_location"}},
                ]
            }
        },
    ],
)
def test_get_condition_composite_invalid_condition(rule: dict):
    """
    The test ensures that the composite produced
    by the factory is the same as the original rule.
    """
    with pytest.raises(RuleFormatError):
        ConditionCompositeFactory.get_condition_composite(rule["conditions"])
