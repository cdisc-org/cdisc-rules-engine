import pytest

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
