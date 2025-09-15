import pytest

from .helpers import create_sql_operators, assert_series_equals


@pytest.mark.parametrize(
    "data,expected_result",
    [
        ({"target": ["Att", "", None]}, [False, True, True]),
        ({"target": [1, 2, None]}, [False, False, True]),
    ],
)
def test_empty(data, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.empty({"target": "target"})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,expected_result",
    [
        ({"target": ["Att", "", None]}, [True, False, False]),
        ({"target": [1, 2, None]}, [True, True, False]),
    ],
)
def test_non_empty(data, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.non_empty({"target": "target"})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        ({"target": ["ABC", "A", ""], "VAR2": ["A", "B", "D"]}, "VAR2", False, [True, False, False]),
        ({"target": ["ABC", "A", ""]}, "A", True, [True, True, False]),
        ({"target": ["ABC", "A", ""]}, "B", True, [False, False, False]),
        ({"target": ["ABC", "A", ""]}, "$constant", False, [True, True, False]),
        ({"target": ["ABC", "A", ""]}, "$list", False, [True, True, False]),
        ({"target": ["", None, "ABC"]}, "A", True, [False, False, True]),
        ({"target": ["ABC", "A", ""]}, "", True, [False, False, False]),
        ({"target": ["ABC", "A", ""]}, ("A", "B"), True, [True, True, False]),
        ({"target": ["ABC", "A", ""]}, ("A",), True, [True, True, False]),
        ({"target": ["ABC", "", "A"]}, ("", "A"), True, [True, False, True]),
        ({"target": ["ABC", "A", ""]}, ["A", "B"], True, [True, True, False]),
        ({"target": ["ABC", "", "A"]}, ["A"], True, [True, False, True]),
    ],
)
def test_starts_with(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.starts_with(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        ({"target": ["ABC", "A", ""], "VAR2": ["C", "A", "D"]}, "VAR2", False, [True, True, False]),
        ({"target": ["ABC", "A", ""]}, "C", True, [True, False, False]),
        ({"target": ["ABC", "A", ""]}, "A", True, [False, True, False]),
        ({"target": ["ABC", "A", ""]}, "$constant", False, [False, True, False]),
        ({"target": ["ABC", "A", ""]}, "$list", False, [False, True, False]),
        ({"target": ["", None, "ABC"]}, "C", True, [False, False, True]),
        ({"target": ["ABC", "A", ""]}, "", True, [False, False, False]),
        ({"target": ["ABC", "A", ""]}, ("C", "A"), True, [True, True, False]),
        ({"target": ["ABC", "", "A"]}, ("", "A"), True, [False, False, True]),
        ({"target": ["ABC", "A", ""]}, ["C", "A"], True, [True, True, False]),
        ({"target": ["ABC", "A", ""]}, ["C"], True, [True, False, False]),
    ],
)
def test_ends_with(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.ends_with(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        ({"target": ["ABC", "A", ""], "VAR2": ["AB", "ABC", "X"]}, "VAR2", False, [True, False, False]),
        ({"target": ["ABC", "A", ""]}, 2, True, [True, False, False]),
        ({"target": ["ABC", "A", ""]}, "AB", True, [True, False, False]),
        ({"target": ["ABC", "A", ""], "VAR2": [2, 3, 1]}, "VAR2", False, [True, False, False]),
        ({"target": ["ABC", "A", ""]}, "$constant", False, [True, False, False]),
        ({"target": ["AB", "A", ""]}, "$number", False, [True, False, False]),
        ({"target": ["ABC", None, "A"]}, 2, True, [True, False, False]),
    ],
)
def test_longer_than(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.longer_than(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        ({"target": ["A", "ABC", ""], "VAR2": ["ABC", "AB", "X"]}, "VAR2", False, [True, False, False]),
        ({"target": ["AB", "A", ""]}, 2, True, [True, True, False]),
        ({"target": ["A", "ABC", ""]}, "ABC", True, [True, True, False]),
        ({"target": ["A", "ABC", ""], "VAR2": [2, 2, 1]}, "VAR2", False, [True, False, False]),
        ({"target": ["A", "ABC", ""]}, "$constant", False, [True, False, False]),
        ({"target": ["A", None, ""]}, 3, True, [True, False, False]),
    ],
)
def test_shorter_than_or_equal_to(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.shorter_than_or_equal_to(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        ({"target": ["ABC", "A", ""], "VAR2": ["AB", "ABC", "X"]}, "VAR2", False, [True, False, False]),
        ({"target": ["AB", "ABC", ""]}, 2, True, [True, True, False]),
        ({"target": ["ABC", "A", ""]}, "AB", True, [True, False, False]),
        ({"target": ["ABC", "A", ""], "VAR2": [2, 2, 1]}, "VAR2", False, [True, False, False]),
        ({"target": ["ABC", "A", ""]}, "$constant", False, [True, True, False]),
        ({"target": ["ABC", None, ""]}, 1, True, [True, False, False]),
    ],
)
def test_longer_than_or_equal_to(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.longer_than_or_equal_to(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        ({"target": ["A", "ABC", ""], "VAR2": ["ABC", "AB", "X"]}, "VAR2", False, [True, False, False]),
        ({"target": ["A", "ABC", ""]}, 3, True, [True, False, False]),
        ({"target": ["A", "ABC", ""]}, "ABC", True, [True, False, False]),
        ({"target": ["A", "ABC", ""], "VAR2": [3, 2, 1]}, "VAR2", False, [True, False, False]),
        ({"target": ["A", "ABC", ""]}, "$number", False, [False, False, False]),
        ({"target": ["A", None, ""]}, 2, True, [True, False, False]),
    ],
)
def test_shorter_than(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.shorter_than(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )

    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        ({"target": ["AB", "A", ""], "VAR2": ["CD", "X", "Y"]}, "VAR2", False, [True, True, False]),
        ({"target": ["AB", "A", ""]}, 2, True, [True, False, False]),
        ({"target": ["AB", "A", ""]}, "AB", True, [True, False, False]),
        ({"target": ["AB", "A", ""], "VAR2": [2, 1, 0]}, "VAR2", False, [True, True, False]),
        ({"target": ["A", "AB", ""]}, "$constant", False, [True, False, False]),
        ({"target": ["AB", None, ""]}, 2, True, [True, False, False]),
    ],
)
def test_has_equal_length(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.has_equal_length(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        ({"target": ["ABC", "A", ""], "VAR2": ["AB", "XY", "Z"]}, "VAR2", False, [True, True, False]),
        ({"target": ["A", "AB", ""]}, 2, True, [True, False, False]),
        ({"target": ["ABC", "A", ""]}, "AB", True, [True, True, False]),
        ({"target": ["ABC", "A", ""], "VAR2": [2, 2, 1]}, "VAR2", False, [True, True, False]),
        ({"target": ["AB", "A", ""]}, "$constant", False, [True, False, False]),
        ({"target": ["ABC", None, ""]}, 2, True, [True, False, False]),
    ],
)
def test_has_not_equal_length(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.has_not_equal_length(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, expected_result)
