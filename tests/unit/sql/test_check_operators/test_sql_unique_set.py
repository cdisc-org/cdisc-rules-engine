import pytest

from .helpers import create_sql_operators, assert_series_equals


@pytest.mark.parametrize(
    "data,target,comparator,expected_result",
    [
        (
            {"COL1": ["A", "B", "C", "D"], "COL2": [1, 2, 3, 4]},
            "COL1",
            "COL2",
            [True, True, True, True],
        ),
        (
            {"COL1": ["A", "A", "B", "B"], "COL2": [1, 1, 2, 2]},
            "COL1",
            "COL2",
            [False, False, False, False],
        ),
        (
            {"COL1": ["A", "A", "B", "C"], "COL2": [1, 1, 2, 3]},
            "COL1",
            "COL2",
            [False, False, True, True],
        ),
        (
            {"STUDYID": ["S1", "S1", "S2", "S3"], "USUBJID": ["U1", "U1", "U2", "U3"]},
            "STUDYID",
            "USUBJID",
            [False, False, True, True],
        ),
        (
            {"COL1": ["A", "B", None, "D"], "COL2": [1, 2, None, 4]},
            "COL1",
            "COL2",
            [True, True, True, True],
        ),
        (
            {"COL1": ["A", "A", "A", "A"], "COL2": [1, 1, 1, 1]},
            "COL1",
            "COL2",
            [False, False, False, False],
        ),
    ],
)
def test_sql_is_unique_set(data, target, comparator, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_unique_set({"target": target, "comparator": comparator})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,target,comparator,expected_result",
    [
        (
            {
                "ARM": ["PLACEBO", "PLACEBO", "DRUG", "DRUG"],
                "VISIT": [1, 1, 1, 2],
                "SEQ": [1, 2, 1, 1],
            },
            "ARM",
            ["VISIT", "SEQ"],
            [True, True, True, True],
        ),
        (
            {
                "ARM": ["PLACEBO", "PLACEBO", "DRUG", "DRUG"],
                "VISIT": [1, 1, 2, 2],
                "SEQ": [1, 1, 1, 1],
            },
            "ARM",
            ["VISIT", "SEQ"],
            [False, False, False, False],
        ),
        (
            {
                "COL1": ["A", "B", "C", "D"],
                "COL2": [1, 2, 3, 4],
                "COL3": ["X", "Y", "Z", "W"],
            },
            ["COL1", "COL2"],
            "COL3",
            [True, True, True, True],
        ),
        (
            {
                "DOMAIN": ["AE", "AE", "DM", "DM"],
                "USUBJID": ["U1", "U1", "U2", "U2"],
                "SEQ": [1, 1, 1, 2],
                "VISIT": [1, 1, 1, 1],
            },
            ["DOMAIN", "USUBJID"],
            ["SEQ", "VISIT"],
            [False, False, True, True],
        ),
        (
            {
                "ARM": ["PLACEBO", "PLACEBO", "A", "A"],
                "TAE": [1, 1, 1, 2],
                "LAE": [1, 2, 1, 2],
            },
            "ARM",
            ["TAE", ["LAE"]],
            [True, True, True, True],
        ),
    ],
)
def test_sql_is_unique_set_multiple_columns(data, target, comparator, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_unique_set({"target": target, "comparator": comparator})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,target,comparator,expected_result",
    [
        (
            {"COL1": ["A", "B", "C", "D"], "COL2": [1, 2, 3, 4]},
            "COL1",
            "COL2",
            [False, False, False, False],
        ),
        (
            {"COL1": ["A", "A", "B", "B"], "COL2": [1, 1, 2, 2]},
            "COL1",
            "COL2",
            [True, True, True, True],
        ),
        (
            {"COL1": ["A", "A", "B", "C"], "COL2": [1, 1, 2, 3]},
            "COL1",
            "COL2",
            [True, True, False, False],
        ),
    ],
)
def test_sql_is_not_unique_set(data, target, comparator, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_not_unique_set({"target": target, "comparator": comparator})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,target,comparator,expected_result",
    [
        (
            {
                "ARM": ["PLACEBO", "PLACEBO", "A", "A"],
                "ARF": [1, 2, 3, 4],
            },
            "--M",
            "--F",
            [True, True, True, True],
        ),
        (
            {
                "ARM": ["PLACEBO", "PLACEBO", "A", "A"],
                "ARF": [1, 1, 2, 2],
            },
            "--M",
            "--F",
            [False, False, False, False],
        ),
    ],
)
def test_sql_is_unique_set_with_prefix_replacement(data, target, comparator, expected_result):
    sql_ops = create_sql_operators(data, extra_config={"column_prefix_map": {"--": "AR"}})
    result = sql_ops.is_unique_set({"target": target, "comparator": comparator})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,target,comparator,expected_result",
    [
        (
            {"COL1": ["A"], "COL2": [1]},
            "COL1",
            "COL2",
            [True],
        ),
        (
            {"COL1": ["A", "B", "C"], "COL2": [1, 2, 3]},
            "COL1",
            "NONEXISTENT",
            [True, True, True],
        ),
        (
            {"COL1": ["", "", ""], "COL2": ["", "", ""]},
            "COL1",
            "COL2",
            [False, False, False],
        ),
    ],
)
def test_sql_is_unique_set_edge_cases(data, target, comparator, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_unique_set({"target": target, "comparator": comparator})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,target,comparator,expected_result",
    [
        (
            {"COL1": ["1", "2", "1", "3"], "COL2": [1, 2, 1, 3]},
            "COL1",
            "COL2",
            [False, True, False, True],
        ),
        (
            {"COL1": ["A|B", "C|D", "A|B", "E|F"], "COL2": ["X", "Y", "X", "Z"]},
            "COL1",
            "COL2",
            [False, True, False, True],
        ),
        (
            {"COL1": ["", "", "A", "B"], "COL2": ["", "X", "Y", "Z"]},
            "COL1",
            "COL2",
            [True, True, True, True],
        ),
    ],
)
def test_sql_is_unique_set_special_values(data, target, comparator, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_unique_set({"target": target, "comparator": comparator})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,target,comparator,expected_result",
    [
        (
            {
                "STUDYID": ["S1", "S1", "S1", "S2"],
                "DOMAIN": ["AE", "AE", "DM", "AE"],
                "USUBJID": ["U1", "U2", "U1", "U1"],
            },
            "STUDYID",
            ["DOMAIN", "USUBJID"],
            [True, True, True, True],
        ),
        (
            {
                "STUDYID": ["S1", "S1", "S2", "S2"],
                "DOMAIN": ["AE", "AE", "DM", "DM"],
                "USUBJID": ["U1", "U1", "U2", "U2"],
            },
            "STUDYID",
            ["DOMAIN", "USUBJID"],
            [False, False, False, False],
        ),
        (
            {
                "COL1": ["A", "A", "B", "B"],
                "COL2": [1, 1, 2, 2],
                "COL3": ["X", "Y", "X", "Y"],
                "COL4": ["P", "Q", "R", "S"],
            },
            ["COL1", "COL2"],
            ["COL3", "COL4"],
            [True, True, True, True],
        ),
    ],
)
def test_sql_is_unique_set_many_columns(data, target, comparator, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_unique_set({"target": target, "comparator": comparator})
    assert_series_equals(result, expected_result)
