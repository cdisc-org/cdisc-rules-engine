import pytest

from .helpers import assert_series_equals, create_sql_operators

PREFIX_MATCHES_REGEX_TEST_DATA = [
    (
        {"target": ["word", "TEST"]},
        "w.*",
        2,
        [True, False],
    ),
    (
        {"target": ["word", "TEST"]},
        "[0-9].*",
        2,
        [False, False],
    ),
    (
        {"target": [224, None]},
        r"^[1-9]{1}\d*$",
        2,
        [True, False],
    ),
    (
        {"target": [-25, 3.14]},
        r"^[1-9]{1}\d*$",
        2,
        [False, False],
    ),
]

PREFIX_NOT_MATCHES_REGEX_TEST_DATA = [
    (
        {"target": ["word", "TEST"]},
        ".*",
        2,
        [False, False],
    ),
    (
        {"target": ["word", "TEST"]},
        "[0-9].*",
        2,
        [True, True],
    ),
    (
        {"target": [224, None]},
        r"^[1-9]{1}\d*$",
        2,
        [False, False],
    ),
    (
        {"target": [-25, 3.14]},
        r"^[1-9]{1}\d*$",
        2,
        [True, True],
    ),
]

SUFFIX_MATCHES_REGEX_TEST_DATA = [
    (
        {"target": ["WORD", "test"]},
        "es.*",
        3,
        [False, True],
    ),
    (
        {"target": ["word", "TEST"]},
        "[0-9].*",
        3,
        [False, False],
    ),
    (
        {"target": [224, None]},
        r"^[1-9]{1}\d*$",
        2,
        [True, False],
    ),
    (
        {"target": [-25, 3.14]},
        r"^[1-9]{1}\d*$",
        2,
        [True, True],
    ),
]

SUFFIX_NOT_MATCHES_REGEX_TEST_DATA = [
    (
        {"target": ["WORD", "test"]},
        ".*",
        3,
        [False, False],
    ),
    (
        {"target": ["word", "TEST"]},
        "[0-9].*",
        3,
        [True, True],
    ),
    (
        {"target": [224, None]},
        r"^[1-9]{1}\d*$",
        2,
        [False, False],
    ),
    (
        {"target": [-25, 3.14]},
        r"^[1-9]{1}\d*$",
        2,
        [False, False],
    ),
]


@pytest.mark.parametrize(
    "data,comparator,length,expected_result",
    PREFIX_MATCHES_REGEX_TEST_DATA,
)
def test_prefix_matches_regex(data, comparator, length, expected_result):
    sql_ops = create_sql_operators(data)

    result = sql_ops.prefix_matches_regex(
        {
            "target": "target",
            "comparator": comparator,
            "prefix": length,
        }
    )

    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,length,expected_result",
    PREFIX_NOT_MATCHES_REGEX_TEST_DATA,
)
def test_not_prefix_matches_regex(data, comparator, length, expected_result):
    sql_ops = create_sql_operators(data)

    result = sql_ops.not_prefix_matches_regex(
        {
            "target": "target",
            "comparator": comparator,
            "prefix": length,
        }
    )

    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,length,expected_result",
    SUFFIX_MATCHES_REGEX_TEST_DATA,
)
def test_suffix_matches_regex(data, comparator, length, expected_result):
    sql_ops = create_sql_operators(data)

    result = sql_ops.suffix_matches_regex(
        {
            "target": "target",
            "comparator": comparator,
            "suffix": length,
        }
    )

    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,length,expected_result",
    SUFFIX_NOT_MATCHES_REGEX_TEST_DATA,
)
def test_not_suffix_matches_regex(data, comparator, length, expected_result):
    sql_ops = create_sql_operators(data)

    result = sql_ops.not_suffix_matches_regex(
        {
            "target": "target",
            "comparator": comparator,
            "suffix": length,
        }
    )

    assert_series_equals(result, expected_result)
