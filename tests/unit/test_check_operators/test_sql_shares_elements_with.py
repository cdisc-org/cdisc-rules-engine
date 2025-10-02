import pytest

from .helpers import create_sql_operators

SHARES_AT_LEAST_ONE_ELEMENT_TEST_DATA = [
    (
        {"target": ["A", "B", "C", "D"], "comparator": ["A", "Y", "Z", "W"]},
        {"target": "target", "comparator": "comparator"},
        True,
    ),
    (
        {"target": ["A", "B", "C", "D"], "comparator": ["X", "Y", "Z", "W"]},
        {"target": "target", "comparator": "comparator"},
        False,
    ),
    (
        {"target": ["A", "A", "A"], "comparator": ["A", "A", "A"]},
        {"target": "target", "comparator": "comparator"},
        True,
    ),
    (
        {"target": ["", "", "A"], "comparator": ["", "B", "C"]},
        {"target": "target", "comparator": "comparator"},
        False,
    ),
    (
        {"target_col": ["A", "B", "C"]},
        {"target": "target_col", "comparator": "$constant"},
        True,
    ),
    (
        {"target_col": ["A", "B", "C", "D"]},
        {"target": "$list", "comparator": "target_col"},
        True,
    ),
    (
        {"dummy": ["value"]},
        {"target": "$list", "comparator": "$list"},
        True,
    ),
]

SHARES_EXACTLY_ONE_ELEMENT_TEST_DATA = [
    (
        {"target": ["A", "B", "C", "D"], "comparator": ["A", "Y", "Z", "W"]},
        {"target": "target", "comparator": "comparator"},
        True,
    ),
    (
        {"target": ["A", "B", "C", "D"], "comparator": ["X", "Y", "Z", "W"]},
        {"target": "target", "comparator": "comparator"},
        False,
    ),
    (
        {"target": ["A", "A", "A"], "comparator": ["A", "A", "A"]},
        {"target": "target", "comparator": "comparator"},
        True,
    ),
    (
        {"target": ["A", "B", "C", "D"], "comparator": ["A", "Y", "C", "W"]},
        {"target": "target", "comparator": "comparator"},
        False,
    ),
    (
        {"dummy": ["value"]},
        {"target": "$constant", "comparator": "$constant"},
        True,
    ),
    (
        {"dummy": ["value"]},
        {"target": "$list", "comparator": "$list"},
        False,
    ),
]

SHARES_NO_ELEMENTS_TEST_DATA = [
    (
        {"target": ["A", "B", "C", "D"], "comparator": ["X", "Y", "Z", "W"]},
        {"target": "target", "comparator": "comparator"},
        True,
    ),
    (
        {"target": ["A", "B", "C", "D"], "comparator": ["A", "Y", "C", "W"]},
        {"target": "target", "comparator": "comparator"},
        False,
    ),
    (
        {"target": ["A", "A", "A"], "comparator": ["A", "A", "A"]},
        {"target": "target", "comparator": "comparator"},
        False,
    ),
    (
        {"target": ["", "", "A"], "comparator": ["", "B", "C"]},
        {"target": "target", "comparator": "comparator"},
        True,
    ),
    (
        {"dummy": ["value"]},
        {"target": "$constant", "comparator": "$date"},
        True,
    ),
    (
        {"target_col": ["X", "Y", "Z"]},
        {"target": "target_col", "comparator": "$constant"},
        True,
    ),
    (
        {"target_col": ["A", "Y", "Z"]},
        {"target": "target_col", "comparator": "$constant"},
        False,
    ),
    (
        {"comparator_col": ["X", "Y", "Z"]},
        {"target": "$constant", "comparator": "comparator_col"},
        True,
    ),
    (
        {"dummy": ["value"]},
        {"target": "$constant", "comparator": "$date"},
        True,
    ),
    (
        {"dummy": ["value"]},
        {"target": "$list", "comparator": "$date"},
        True,
    ),
    (
        {"dummy": ["value"]},
        {"target": "$list", "comparator": "$list"},
        False,
    ),
]


@pytest.mark.parametrize(
    "data,params,expected_result",
    SHARES_AT_LEAST_ONE_ELEMENT_TEST_DATA,
)
def test_sql_shares_at_least_one_element_with(data, params, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.shares_at_least_one_element_with(params)
    assert result == expected_result


@pytest.mark.parametrize(
    "data,params,expected_result",
    SHARES_EXACTLY_ONE_ELEMENT_TEST_DATA,
)
def test_sql_shares_exactly_one_element_with(data, params, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.shares_exactly_one_element_with(params)
    assert result == expected_result


@pytest.mark.parametrize(
    "data,params,expected_result",
    SHARES_NO_ELEMENTS_TEST_DATA,
)
def test_sql_shares_no_elements_with(data, params, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.shares_no_elements_with(params)
    assert result == expected_result


SHARES_EDGE_CASES = [
    (
        {"target": ["A"], "comparator": ["A"]},
        True,
        True,
        False,
    ),
    (
        {"target": ["A"], "comparator": ["B"]},
        False,
        False,
        True,
    ),
    (
        {"target": ["A", "b"], "comparator": ["a", "B"]},
        False,
        False,
        True,
    ),
    (
        {"target": ["1", "2", "3"], "comparator": ["1", "4", "5"]},
        True,
        True,
        False,
    ),
    (
        {"target": ["A", "B", "C"], "comparator": ["A", "B", "X"]},
        True,
        False,
        False,
    ),
]


@pytest.mark.parametrize(
    "data,expected_at_least_one,expected_exactly_one,expected_no_elements",
    SHARES_EDGE_CASES,
)
def test_sql_shares_elements_edge_cases(data, expected_at_least_one, expected_exactly_one, expected_no_elements):
    sql_ops = create_sql_operators(data)

    result_at_least_one = sql_ops.shares_at_least_one_element_with({"target": "target", "comparator": "comparator"})
    result_exactly_one = sql_ops.shares_exactly_one_element_with({"target": "target", "comparator": "comparator"})
    result_no_elements = sql_ops.shares_no_elements_with({"target": "target", "comparator": "comparator"})

    assert result_at_least_one is expected_at_least_one
    assert result_exactly_one is expected_exactly_one
    assert result_no_elements is expected_no_elements
