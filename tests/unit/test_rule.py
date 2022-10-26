import pytest
from cdisc_rules_engine.models.rule import Rule


@pytest.mark.parametrize(
    "condition, expected_additional_keys",
    [
        ({"operator": "test", "name": "IDVAR", "prefix": 10}, ["prefix"]),
        ({"operator": "test", "name": "IDVAR", "suffix": 10}, ["suffix"]),
        (
            {"operator": "test", "name": "IDVAR", "date_component": "year"},
            ["date_component"],
        ),
        ({"operator": "test", "name": "IDVAR", "context": "RDOMAIN"}, ["context"]),
        (
            {"operator": "test", "name": "IDVAR", "value_is_literal": False},
            ["value_is_literal"],
        ),
    ],
)
def test_build_conditions(condition, expected_additional_keys):
    result = Rule.build_condition(condition, "get_dataset")
    value = result.get("value")
    assert len(value.keys()) == 2 + len(expected_additional_keys)
    assert value["target"] == condition["name"]
    for key in expected_additional_keys:
        assert value[key] == condition[key]


def test_parse_conditions_without_check_data_provided():
    conditions = {}
    with pytest.raises(ValueError) as err:
        Rule.parse_conditions(conditions)
        assert err.args[0] == "No check data provided"


def test_valid_parse_conditions():
    conditions = {"all": [{"name": "IDVAR", "operator": "not_equal_to", "value": 10}]}
    parsed_conditions = Rule.parse_conditions(conditions)
    assert "all" in parsed_conditions
    assert len(parsed_conditions["all"]) == 1
    condition = parsed_conditions["all"][0]
    assert condition.get("name") == "get_dataset"
    assert condition.get("operator") == conditions["all"][0]["operator"]
    assert condition["value"]["target"] == conditions["all"][0]["name"]
    assert condition["value"]["comparator"] == conditions["all"][0]["value"]


def test_valid_parse_actions():
    actions = {"Message": "Great job!"}
    parsed_actions = Rule.parse_actions(actions)
    assert isinstance(parsed_actions, list)
    assert len(parsed_actions) == 1
    assert parsed_actions[0]["name"] == "generate_dataset_error_objects"
    assert parsed_actions[0]["params"]["message"] == actions["Message"]


@pytest.mark.parametrize(
    "match_datasets, expected_result",
    [
        (
            [{"Name": "AA", "Keys": ["USUBJID"]}],
            [{"domain_name": "AA", "match_key": ["USUBJID"]}],
        ),
        (
            [{"Name": "SUPPEC", "Keys": ["USUBJID"], "Is_Relationship": True}],
            [
                {
                    "domain_name": "SUPPEC",
                    "match_key": ["USUBJID"],
                    "relationship_columns": {
                        "column_with_names": "IDVAR",
                        "column_with_values": "IDVARVAL",
                    },
                }
            ],
        ),
    ],
)
def test_parse_datasets(match_datasets, expected_result):
    assert Rule.parse_datasets(match_datasets) == expected_result


@pytest.mark.parametrize(
    "yaml, output",
    [
        (
            {"not": {"all": [{"operator": "test", "name": "IDVAR", "value": 5}]}},
            {
                "not": {
                    "all": [
                        {
                            "name": "get_dataset",
                            "operator": "test",
                            "value": {"target": "IDVAR", "comparator": 5},
                        }
                    ]
                }
            },
        ),
        (
            {
                "all": [
                    {
                        "not": {
                            "any": [{"operator": "test", "name": "IDVAR", "value": 5}]
                        }
                    }
                ]
            },
            {
                "all": [
                    {
                        "not": {
                            "any": [
                                {
                                    "name": "get_dataset",
                                    "operator": "test",
                                    "value": {"target": "IDVAR", "comparator": 5},
                                }
                            ]
                        }
                    }
                ]
            },
        ),
    ],
)
def test_parse_conditions_not_condition(yaml, output):
    assert Rule.parse_conditions(yaml) == output
