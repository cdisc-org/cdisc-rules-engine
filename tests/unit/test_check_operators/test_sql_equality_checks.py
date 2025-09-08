import pytest


from .helpers import assert_series_equals, create_sql_operators


@pytest.mark.parametrize(
    "data,comparator,is_literal,expected_result",
    [
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "VAR2",
            False,
            [True, True, True],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            False,
            [False, True, False],
        ),
        (
            {"target": ["A", "B", "VAR2"], "VAR2": ["A", "B", "C"]},
            "VAR2",
            True,
            [False, False, True],
        ),
        (
            {"target": [2, 1.0, 1]},
            1,
            True,
            [False, True, True],
        ),
        (
            {"target": ["A", "B", ""], "VAR2": ["", "", ""]},
            "VAR2",
            False,
            [False, False, False],
        ),
        (
            {"target": ["A", "B", None], "VAR2": ["A", "B", "C"]},
            "",
            True,
            [False, False, False],
        ),
        (
            {"target": ["A", "B", "C"]},
            "$constant",
            False,
            [True, False, False],
        ),
    ],
)
def test_equal_to(data, comparator, is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.equal_to({"target": "target", "comparator": comparator, "value_is_literal": is_literal})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,operator,expected_result",
    [
        (
            {
                "target": ["A", "B", "C"],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": ["D", "D", "C"],
                "AESEQ": ["E", "B", "E"],
            },
            "IDVAR",
            "equal_to",
            [False, True, True],
        ),
        (
            {
                "target": ["A", "B", "C"],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": ["D", "D", "C"],
                "AESEQ": ["E", "B", "E"],
            },
            "IDVAR",
            "not_equal_to",
            [True, False, False],
        ),
    ],
)
def test_equality_operators_value_is_reference(data, comparator, operator, expected_result):
    """Test equal_to and not_equal_to operators with value_is_reference=True for dynamic column comparison."""
    sql_ops = create_sql_operators(data)
    if operator == "equal_to":
        result = sql_ops.equal_to({"target": "target", "comparator": comparator, "value_is_reference": True})
    else:
        result = sql_ops.not_equal_to({"target": "target", "comparator": comparator, "value_is_reference": True})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,operator,expected_result",
    [
        (
            {
                "target": ["320", "2", "15"],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": [320, 21, 15],
                "AESEQ": [1, 2, 1],
            },
            "IDVAR",
            "equal_to",
            [True, True, True],
        ),
        (
            {
                "target": ["320", "2", "15"],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": [320, 21, 15],
                "AESEQ": [1, 2, 1],
            },
            "IDVAR",
            "not_equal_to",
            [False, False, False],
        ),
        (
            {
                "target": ["999", "5", "100"],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": [320, 21, 15],
                "AESEQ": [1, 2, 1],
            },
            "IDVAR",
            "equal_to",
            [False, False, False],
        ),
        (
            {
                "target": ["1", "2", "3"],
                "IDVAR": ["FLOATCOL", "FLOATCOL", "FLOATCOL"],
                "FLOATCOL": [1.0, 2.0, 3.0],
            },
            "IDVAR",
            "equal_to",
            [True, True, True],
        ),
    ],
)
def test_equality_operators_type_insensitive(data, comparator, operator, expected_result):
    sql_ops = create_sql_operators(data)

    if operator == "equal_to":
        result = sql_ops.equal_to(
            {
                "target": "target",
                "comparator": comparator,
                "value_is_reference": True,
                "type_insensitive": True,
            }
        )
    else:
        result = sql_ops.not_equal_to(
            {
                "target": "target",
                "comparator": comparator,
                "value_is_reference": True,
                "type_insensitive": True,
            }
        )

    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["A", "B", "", "", None], "VAR2": ["A", "B", "C", None, None]},
            "VAR2",
            [False, False, True, False, False],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            [True, False, True],
        ),
        (
            {"target": ["A", "", None]},
            None,
            [True, False, False],
        ),
        (
            {"target": ["A", "a", "b"]},
            "$constant",
            [False, True, True],
        ),
    ],
)
def test_not_equal_to(data, comparator, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.not_equal_to({"target": "target", "comparator": comparator})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["A", "B", "C"], "VAR2": ["a", "b", "c"]},
            "VAR2",
            [True, True, True],
        ),
        (
            {"target": ["A", "b", "B"], "VAR2": ["A", "B", "C"]},
            "B",
            [False, True, True],
        ),
        (
            {"target": ["A", "a", "b"]},
            "$constant",
            [True, True, False],
        ),
    ],
)
def test_equal_to_case_insensitive(data, comparator, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.equal_to_case_insensitive({"target": "target", "comparator": comparator})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["A", "B", "C"], "VAR2": ["a", "b", "c"]},
            "VAR2",
            [False, False, False],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "b",
            [True, False, True],
        ),
        (
            {"target": ["A", "a", "b"]},
            "$constant",
            [False, False, True],
        ),
    ],
)
def test_not_equal_to_case_insensitive(data, comparator, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.not_equal_to_case_insensitive({"target": "target", "comparator": comparator})
    assert_series_equals(result, expected_result)
