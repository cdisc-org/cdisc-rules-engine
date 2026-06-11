import pytest

from .helpers import assert_series_equals, create_sql_operators

equals_string_part_test_data = [
    (
        {"VAR2": ["blaAtt", "yyyBtt", "aaaCtt"], "target": ["A", "B", "D"]},
        "target",
        "VAR2",
        ".{3}(.).*",
        False,
        [True, True, False],
    ),
    (
        {"comparator_col": ["blaAtt", "yyyBtt", "aaaCtt"], "target": ["A", "B", "C"]},
        "target",
        "comparator_col",
        ".{3}(.).*",
        False,
        [True, True, True],
    ),
    (
        {"VAR2": ["abc", "def", "ghi"], "target": ["X", "Y", "Z"]},
        "target",
        "VAR2",
        "[0-9]+",
        False,
        [False, False, False],
    ),
    (
        {"VAR2": ["abc123def", "xyz456ghi", "test789end"], "target": ["123", "456", "999"]},
        "target",
        "VAR2",
        "[0-9]+",
        False,
        [True, True, False],
    ),
    (
        {"VAR2": [None, "", "blaAtt"], "target": ["A", "B", "A"]},
        "target",
        "VAR2",
        ".{3}(.).*",
        False,
        [False, False, True],
    ),
    (
        {"VAR2": ["blaAtt", "yyyBtt", "aaaCtt"], "target": ["X", "Y", "C"]},
        "target",
        "VAR2",
        ".{3}(.).*",
        False,
        [False, False, True],
    ),
    (
        {"VAR2": [None, "", "aaaCtt"], "target": ["A", "A", "C"]},
        "target",
        "VAR2",
        ".{3}(.).*",
        False,
        [False, False, True],
    ),
]

does_not_equal_string_part_test_data = [
    (
        {"VAR2": ["blaAtt", "yyyBtt", "aaaCtt"], "target": ["A", "B", "D"]},
        "target",
        "VAR2",
        ".{3}(.).*",
        False,
        [False, False, True],
    ),
    (
        {"comparator_col": ["blaAtt", "yyyBtt", "aaaCtt"], "target": ["A", "B", "C"]},
        "target",
        "comparator_col",
        ".{3}(.).*",
        False,
        [False, False, False],
    ),
    (
        {"VAR2": [None, "", "blaAtt"], "target": ["A", "B", "A"]},
        "target",
        "VAR2",
        ".{3}(.).*",
        False,
        [False, False, False],
    ),
]


@pytest.mark.parametrize(
    "data,target,comparator,regex,value_is_literal,expected_result",
    equals_string_part_test_data,
)
def test_equals_string_part(data, target, comparator, regex, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)

    result = sql_ops.equals_string_part(
        {
            "target": target,
            "comparator": comparator,
            "regex": regex,
            "value_is_literal": value_is_literal,
        }
    )

    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,target,comparator,regex,value_is_literal,expected_result",
    does_not_equal_string_part_test_data,
)
def test_does_not_equal_string_part(data, target, comparator, regex, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)

    result = sql_ops.does_not_equal_string_part(
        {
            "target": target,
            "comparator": comparator,
            "regex": regex,
            "value_is_literal": value_is_literal,
        }
    )

    assert_series_equals(result, expected_result)
