import pytest
import pandas as pd

from .helpers import create_sql_operators, assert_series_equals


@pytest.mark.parametrize(
    "data, comparator, within, expected_result",
    [
        (
            {
                "USUBJID": [1, 1, 1, 2, 2, 2],
                "SEQ": [1, 2, 3, 4, 5, 6],
                "target": [
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP3",
                    "AEHOSP2",
                    "AEHOSP2",
                ],
            },
            1,
            "USUBJID",
            [True, True, True, False, True, True],
        ),
    ],
)
def test_present_on_multiple_rows_within(data, comparator, within, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.present_on_multiple_rows_within({"target": "target", "comparator": comparator, "within": within})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data, expected_result",
    [
        (
            {
                "target": [
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP3",
                    "AEHOSP2",
                    "AEHOSP2",
                ],
            },
            [True, True, True, True, True, True],
        ),
    ],
)
def test_has_different_values(data, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.has_different_values({"target": "target"})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data, expected_result",
    [
        (
            {
                "target": [
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP3",
                    "AEHOSP2",
                    "AEHOSP2",
                ],
            },
            [False, False, False, False, False, False],
        ),
        (
            {
                "target": [
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                ],
            },
            [True, True, True, True, True, True],
        ),
    ],
)
def test_has_same_values(data, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.has_same_values({"target": "target"})
    assert_series_equals(result, expected_result)


HAS_NEXT_CORRESPONDING_RECORD_DATA = [
    (
        {
            "USUBJID": [789, 789, 789, 789, 790, 790, 790, 790],
            "SESEQ": [1, 2, 3, 4, 5, 6, 7, 8],
            "SEENDTC": [
                "2006-06-03T10:32",
                "2006-06-10T09:47",
                "2006-06-17",
                "2006-06-17",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
                "2006-06-17",
            ],
            "SESTDTC": [
                "2006-06-01",
                "2006-06-03T10:32",
                "2006-06-10T09:47",
                "2006-06-17",
                "2006-06-01",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
            ],
        },
        [True, True, True, True, True, True, True, True],
    ),
    (
        {
            "USUBJID": [789, 789, 789, 789, 790, 790, 790, 790],
            "SESEQ": [1, 2, 3, 4, 5, 6, 7, 8],
            "SEENDTC": [
                "2006-06-03T10:32",
                "2006-06-10T09:47",
                "2006-06-17",
                "2006-06-17",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
                "2006-06-17",
            ],
            "SESTDTC": [
                "2006-06-01",
                "2010-08-03",
                "2008-08",
                "2006-06-17T10:20",
                "2006-06-01",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
            ],
        },
        [False, False, False, True, True, True, True, True],
    ),
]


@pytest.mark.parametrize("data, expected_result", HAS_NEXT_CORRESPONDING_RECORD_DATA)
def test_has_next_corresponding_record(data, expected_result):
    """Test for has_next_corresponding_record operator."""
    sql_ops = create_sql_operators(data)
    result = sql_ops.has_next_corresponding_record(
        {
            "target": "SEENDTC",
            "comparator": "SESTDTC",
            "within": "USUBJID",
            "ordering": "SESEQ",
        }
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize("data, expected_result", HAS_NEXT_CORRESPONDING_RECORD_DATA)
def test_does_not_have_next_corresponding_record(data, expected_result):
    """Test for has_next_corresponding_record operator."""
    sql_ops = create_sql_operators(data)
    result = sql_ops.does_not_have_next_corresponding_record(
        {
            "target": "SEENDTC",
            "comparator": "SESTDTC",
            "within": "USUBJID",
            "ordering": "SESEQ",
        }
    )
    assert_series_equals(result, ~pd.Series(expected_result))


EMPTY_WITHIN_EXCEPT_LAST_ROW_DATA = [
    (
        {
            "USUBJID": [1, 1, 1, 2, 2, 2],
            "valid": [
                "2020-10-10",
                "2020-10-10",
                "2020-10-10",
                "2021",
                "2021",
                "2021",
            ],
            "invalid": [
                "2020-10-10",
                None,
                None,
                "2020",
                "2020",
                None,
            ],
            "SEQ": [1, 2, 3, 1, 2, 3],
        },
        {"target": "valid", "comparator": "USUBJID"},
        [False, False, False, False, False, False],
    ),
    (
        {
            "USUBJID": [1, 1, 1, 2, 2, 2],
            "valid": [
                "2020-10-10",
                "2020-10-10",
                "2020-10-10",
                "2021",
                "2021",
                "2021",
            ],
            "invalid": [
                "2020-10-10",
                None,
                None,
                "2020",
                "2020",
                None,
            ],
            "SEQ": [1, 2, 3, 1, 2, 3],
        },
        {"target": "invalid", "comparator": "USUBJID"},
        [False, True, False, False, False, False],
    ),
    (
        {
            "USUBJID": [789, 789, 789, 789, 790, 790, 790, 790],
            "SESEQ": [1, 2, 3, 4, 5, 6, 7, 8],
            "SEENDTC": [
                "2006-06-03T10:32",
                "2006-06-10T09:47",
                "2006-06-17",
                "2006-06-18",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
                "2006-06-18",
            ],
            "SESTDTC": [
                "2006-06-01",
                "2006-06-03T10:32",
                "2006-06-10T09:47",
                "2006-06-17",
                "2006-06-01",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
            ],
        },
        {"target": "SEENDTC", "comparator": "USUBJID", "ordering": "SESTDTC"},
        [False, False, False, False, False, False, False, False],
    ),
    (
        {
            "USUBJID": [789, 789, 789, 789, 790, 790, 790, 790],
            "SESEQ": [1, 2, 3, 4, 5, 6, 7, 8],
            "SEENDTC": [
                "",
                "2006-06-10T09:47",
                "2006-06-17",
                "2006-06-18",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
                "2006-06-18",
            ],
            "SESTDTC": [
                "2006-06-01",
                "2006-06-03T10:32",
                "2006-06-10T09:47",
                "2006-06-17",
                "2006-06-01",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
            ],
        },
        {"target": "SEENDTC", "comparator": "USUBJID", "ordering": "SESTDTC"},
        [True, False, False, False, False, False, False, False],
    ),
    (
        {
            "USUBJID": [1, 1, 1, 2, 2, 2],
            "VISITNUM": [1, 2, 3, 1, 2, 3],
            "VISIT": [
                "SCREENING",
                "",
                "",
                "SCREENING",
                "TREATMENT",
                "",
            ],
            "VISITDTC": [
                "2020-01-01",
                "2020-01-15",
                "2020-02-01",
                "2020-01-01",
                "2020-01-15",
                "2020-02-01",
            ],
        },
        {"target": "VISIT", "comparator": "USUBJID", "ordering": "VISITDTC"},
        [False, True, False, False, False, False],
    ),
    (
        {
            "USUBJID": [1, 1, 1, 2, 2, 2],
            "VISITNUM": [1, 2, 3, 1, 2, 3],
            "VISIT": [
                "",
                "",
                "FOLLOW-UP",
                "",
                None,
                "FOLLOW-UP",
            ],
            "VISITDTC": [
                "2020-01-01",
                "2020-01-15",
                "2020-02-01",
                "2020-01-01",
                "2020-01-15",
                "2020-02-01",
            ],
        },
        {"target": "VISIT", "comparator": "USUBJID", "ordering": "VISITDTC"},
        [True, True, False, True, True, False],
    ),
]


@pytest.mark.parametrize("data, params, expected_result", EMPTY_WITHIN_EXCEPT_LAST_ROW_DATA)
def test_empty_within_except_last_row(data, params, expected_result):
    """Test for empty_within_except_last_row operator."""
    sql_ops = create_sql_operators(data)
    result = sql_ops.empty_within_except_last_row(params)
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize("data, params, expected_result", EMPTY_WITHIN_EXCEPT_LAST_ROW_DATA)
def test_non_empty_within_except_last_row(data, params, expected_result):
    """Test for non_empty_within_except_last_row operator (inverse of empty_within_except_last_row)."""
    sql_ops = create_sql_operators(data)
    result = sql_ops.non_empty_within_except_last_row(params)
    assert_series_equals(result, ~pd.Series(expected_result))
