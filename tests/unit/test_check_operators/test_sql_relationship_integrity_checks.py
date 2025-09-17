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
