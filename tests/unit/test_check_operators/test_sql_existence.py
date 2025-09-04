import pytest

from .helpers import assert_series_equals, create_sql_operators_with_config


@pytest.mark.parametrize(
    "target, expected_result",
    [
        ("var1", [True, True, True]),
        ("--r1", [True, True, True]),
        # ("nested_var", [True, True, True]),
        ("invalid", [False, False, False]),
        # ("a", [True, True, True]),
        # ("f", [True, True, True]),
        ("x", [False, False, False]),
        ("non_nested_value", [True, True, True]),
    ],
)
def test_exists(target, expected_result):
    data = {
        "var1": [1, 2, 4],
        "var2": [3, 5, 6],
        # "nested_var": [["a", "b", "c"], ["d", "e"], ["f", "nested_var", "g"]],
        "non_nested_value": ["h", "i", "j"],
    }
    sql_ops = create_sql_operators_with_config(data, {"column_prefix_map": {"--": "va"}})
    result = sql_ops.exists({"target": target})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "target, expected_result",
    [
        ("var1", [False, False, False]),
        ("--r1", [False, False, False]),
        # ("nested_var", [False, False, False]),
        ("invalid", [True, True, True]),
        # ("a", [False, False, False]),
        # ("f", [False, False, False]),
        ("x", [True, True, True]),
        ("non_nested_value", [False, False, False]),
    ],
)
def test_not_exists(target, expected_result):
    data = {
        "var1": [1, 2, 4],
        "var2": [3, 5, 6],
        # "nested_var": [["a", "b", "c"], ["d", "e"], ["f", "nested_var", "g"]],
        "non_nested_value": ["h", "i", "j"],
    }
    sql_ops = create_sql_operators_with_config(data, {"column_prefix_map": {"--": "va"}})
    result = sql_ops.not_exists({"target": target})
    assert_series_equals(result, expected_result)
