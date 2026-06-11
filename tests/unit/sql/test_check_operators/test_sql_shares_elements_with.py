import pytest
import pandas as pd

from .helpers import create_sql_operators

SHARES_AT_LEAST_ONE_ELEMENT_TEST_DATA = [
    (
        {"dummy": ["value"]},
        {"target": "$list", "comparator": "$list"},
        [True],
    ),
    (
        {"col1": ["A", "B", "C"], "col2": ["B", "C", "D"]},
        {"target": "col1", "comparator": "col2"},
        [True, True, True],
    ),
    (
        {"col1": ["A", "B"], "col2": ["B", "C"]},
        {"target": "col1", "comparator": "col2"},
        [True, True],
    ),
    (
        {"col1": ["A", "B", "C"], "col2": ["A", "B", "D"]},
        {"target": "col1", "comparator": "col2"},
        [True, True, True],
    ),
    (
        {"col1": ["A", "B"], "col2": ["A", "B"]},
        {"target": "col1", "comparator": "col2"},
        [True, True],
    ),
    (
        {"col1": ["A", "B", "C", "D"]},
        {"target": "$list", "comparator": "col1"},
        [True, True, True, True],
    ),
    (
        {"col1": ["A", "C", "D"]},
        {"target": "col1", "comparator": "$list"},
        [True, True, True],
    ),
    (
        {"col1": ["A", "", None, "B"], "col2": ["A", "C", "", "D"]},
        {"target": "col1", "comparator": "col2"},
        [True, True, True, True],
    ),
    (
        {"col1": ["", None, "A"], "col2": ["", "A", "B"]},
        {"target": "col1", "comparator": "col2"},
        [True, True, True],
    ),
    # Simple vs Simple test cases
    (
        {"dummy": ["value"]},
        {"target": "$constant", "comparator": "$constant"},
        [True],
    ),
    (
        {"dummy": ["value"]},
        {"target": "A", "comparator": "A"},
        [True],
    ),
    (
        {"dummy": ["value"]},
        {"target": "$constant", "comparator": "$constant"},
        [True],
    ),
]

SHARES_EXACTLY_ONE_ELEMENT_TEST_DATA = [
    (
        {"dummy": ["value"]},
        {"target": "$list", "comparator": "$list"},
        [False],
    ),
    (
        {"col1": ["A", "B"], "col2": ["B", "C"]},
        {"target": "col1", "comparator": "col2"},
        [True, True],
    ),
    (
        {"col1": ["A", "C", "D"]},
        {"target": "col1", "comparator": "$list"},
        [True, True, True],
    ),
    (
        {"col1": ["A", "", None, "B"], "col2": ["A", "C", "", "D"]},
        {"target": "col1", "comparator": "col2"},
        [True, True, True, True],
    ),
    (
        {"col1": ["", None, "A"], "col2": ["", "A", "B"]},
        {"target": "col1", "comparator": "col2"},
        [True, True, True],
    ),
    # Simple vs Simple test cases
    (
        {"dummy": ["value"]},
        {"target": "$constant", "comparator": "$constant"},
        [True],
    ),
    (
        {"dummy": ["value"]},
        {"target": "X", "comparator": "Y"},
        [False],
    ),
    (
        {"dummy": ["value"]},
        {"target": "A", "comparator": "A"},
        [True],
    ),
]

SHARES_NO_ELEMENTS_TEST_DATA = [
    (
        {"dummy": ["value"]},
        {"target": "$list", "comparator": "$list"},
        [False],
    ),
    (
        {"col1": ["A", "B"], "col2": ["C", "D"]},
        {"target": "col1", "comparator": "col2"},
        [True, True],
    ),
    (
        {"col1": ["C", "D", "E"]},
        {"target": "$list", "comparator": "col1"},
        [True, True, True],
    ),
    (
        {"col1": ["C", "D", "E"]},
        {"target": "col1", "comparator": "$list"},
        [True, True, True],
    ),
    (
        {"col1": ["", None], "col2": ["", None]},
        {"target": "col1", "comparator": "col2"},
        [True, True],
    ),
    # Simple vs Simple test cases
    (
        {"dummy": ["value"]},
        {"target": "A", "comparator": "B"},
        [True],
    ),
    (
        {"dummy": ["value"]},
        {"target": "X", "comparator": "Y"},
        [True],
    ),
    (
        {"dummy": ["value"]},
        {"target": "$constant", "comparator": "$constant"},
        [False],
    ),
]


@pytest.mark.parametrize(
    "data,params,expected_result",
    SHARES_AT_LEAST_ONE_ELEMENT_TEST_DATA,
)
def test_sql_shares_at_least_one_element_with(data, params, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.shares_at_least_one_element_with(params)
    expected_series = pd.Series(expected_result, dtype=bool)
    pd.testing.assert_series_equal(result, expected_series)


@pytest.mark.parametrize(
    "data,params,expected_result",
    SHARES_EXACTLY_ONE_ELEMENT_TEST_DATA,
)
def test_sql_shares_exactly_one_element_with(data, params, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.shares_exactly_one_element_with(params)
    expected_series = pd.Series(expected_result, dtype=bool)
    pd.testing.assert_series_equal(result, expected_series)


@pytest.mark.parametrize(
    "data,params,expected_result",
    SHARES_NO_ELEMENTS_TEST_DATA,
)
def test_sql_shares_no_elements_with(data, params, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.shares_no_elements_with(params)
    expected_series = pd.Series(expected_result, dtype=bool)
    pd.testing.assert_series_equal(result, expected_series)


SHARES_EDGE_CASES = [
    (
        {"dummy": ["value"]},
        {"target": "$constant", "comparator": "$constant"},
        [True],
        [True],
        [False],
    ),
    (
        {"dummy": ["value"]},
        {"target": "$constant", "comparator": "$date"},
        [False],
        [False],
        [True],
    ),
    (
        {"dummy": ["value"]},
        {"target": "$list", "comparator": "$constant"},
        [True],
        [True],
        [False],
    ),
    (
        {"dummy": ["value"]},
        {"target": "$list", "comparator": "$date"},
        [False],
        [False],
        [True],
    ),
]


@pytest.mark.parametrize(
    "data,params,expected_at_least_one,expected_exactly_one,expected_no_elements",
    SHARES_EDGE_CASES,
)
def test_sql_shares_elements_edge_cases(
    data, params, expected_at_least_one, expected_exactly_one, expected_no_elements
):
    sql_ops = create_sql_operators(data)

    result_at_least_one = sql_ops.shares_at_least_one_element_with(params)
    result_exactly_one = sql_ops.shares_exactly_one_element_with(params)
    result_no_elements = sql_ops.shares_no_elements_with(params)

    # Convert expected lists to Series for proper comparison
    expected_at_least_one_series = pd.Series(expected_at_least_one, dtype=bool)
    expected_exactly_one_series = pd.Series(expected_exactly_one, dtype=bool)
    expected_no_elements_series = pd.Series(expected_no_elements, dtype=bool)

    pd.testing.assert_series_equal(result_at_least_one, expected_at_least_one_series)
    pd.testing.assert_series_equal(result_exactly_one, expected_exactly_one_series)
    pd.testing.assert_series_equal(result_no_elements, expected_no_elements_series)
