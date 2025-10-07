import pytest

from .helpers import assert_series_equals, create_sql_operators


@pytest.mark.parametrize(
    "data,target,expected_result",
    [
        (
            {
                "USUBJID": [1, 1, 1, 1],
                "TSVAL": [None, None, "another value", None],
                "TSVAL1": ["value", "value", "value", None],
                "TSVAL2": [None, "value 2", "value 2", None],
            },
            "TSVAL",
            [True, True, False, False],
        ),
        (
            {
                "USUBJID": [1, 1, 1, 1],
                "TSVAL": ["value", None, "another value", None],
                "TSVAL1": ["value", None, "value", "value"],
                "TSVAL2": ["value 2", "value 2", "value 2", None],
                "TSVAL3": ["value 3", "value 3", None, "value 3"],
            },
            "TSVAL",
            [False, True, False, True],
        ),
        (
            {
                "USUBJID": [1, 1, 1, 1],
                "TSVAL": ["value", "value", "value", "value"],
                "TSVAL1": ["value", "value", "value", "value"],
                "TSVAL2": ["value 2", "value 2", "value 2", "value 2"],
            },
            "TSVAL",
            [False, False, False, False],
        ),
        (
            {
                "USUBJID": [1],
                "TSVAL": [None],
                "TSVAL1": [None],
                "TSVAL2": ["value"],
            },
            "TSVAL",
            [True],
        ),
        (
            {
                "USUBJID": [1],
                "TSVAL": ["value"],
            },
            "TSVAL",
            [False],
        ),
    ],
)
def test_inconsistent_enumerated_columns(data, target, expected_result):
    """Test the inconsistent_enumerated_columns SQL operator."""
    sql_ops = create_sql_operators(data)
    result = sql_ops.inconsistent_enumerated_columns({"target": target})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,target,expected_result",
    [
        # Test with different variable names
        (
            {
                "USUBJID": [1, 2, 3],
                "AEVAL": [None, "value", "value"],
                "AEVAL1": ["populated", None, "value1"],
                "AEVAL2": [None, "populated", "value2"],
            },
            "AEVAL",
            [True, True, False],
        ),
        # Test with numeric values
        (
            {
                "USUBJID": [1, 2, 3],
                "LBVAL": [None, 1, 2],
                "LBVAL1": [10, None, 11],
                "LBVAL2": [None, 20, 22],
            },
            "LBVAL",
            [True, True, False],
        ),
        # Test case-insensitive matching
        (
            {
                "USUBJID": [1, 2],
                "tsval": [None, "value"],
                "tsval1": ["value1", "value1"],
            },
            "TSVAL",  # uppercase target should match lowercase columns
            [True, False],
        ),
        # No matching columns for target variable
        (
            {
                "USUBJID": [1],
                "OTHERCOL": ["value"],
            },
            "TSVAL",
            [False],
        ),
        # Empty strings treated as non-populated
        (
            {
                "USUBJID": [1, 2, 3],
                "TSVAL": ["", None, "value"],
                "TSVAL1": ["value", "value", ""],
                "TSVAL2": [None, "", "value"],
            },
            "TSVAL",
            [True, True, True],
        ),
        # Sequential enumeration with gaps
        (
            {
                "USUBJID": [1, 2, 3, 4],
                "TSVAL": ["value", "", None, "value"],
                "TSVAL1": ["value1", "value1", "value1", ""],
                "TSVAL3": ["value3", "", None, "value3"],
            },
            "TSVAL",
            [False, True, True, True],
        ),
    ],
)
def test_inconsistent_enumerated_columns_edge_cases(data, target, expected_result):
    """Test various scenarios for inconsistent_enumerated_columns operator."""
    sql_ops = create_sql_operators(data)
    result = sql_ops.inconsistent_enumerated_columns({"target": target})
    assert_series_equals(result, expected_result)
