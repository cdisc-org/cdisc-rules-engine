from cdisc_rules_engine.rule_operators.dask_dataframe_operators import DaskDataframeType
import dask.dataframe as dd
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "btt", "lll"]},
            "VAR2",
            [True, False, False],
        ),
        (
            {"target": [["A", "B", "C"], ["A", "B", "L"], ["L", "Q", "R"]]},
            "L",
            [False, True, True],
        ),
    ],
)
def test_contains(data, comparator, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.contains({"target": "target", "comparator": comparator})
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["a", "btt", "lll"]},
            "VAR2",
            [True, True, False],
        ),
        (
            {"target": [["A", "B", "C"], ["A", "B", "L"], ["L", "Q", "R"]]},
            "l",
            [False, True, True],
        ),
    ],
)
def test_contains_case_insensitive(data, comparator, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.contains_case_insensitive(
        {"target": "target", "comparator": comparator}
    )
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "btt", "lll"]},
            "VAR2",
            [False, True, True],
        ),
        (
            {"target": [["A", "B", "C"], ["A", "B", "L"], ["L", "Q", "R"]]},
            "L",
            [True, False, False],
        ),
    ],
)
def test_does_not_contain(data, comparator, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.does_not_contain(
        {"target": "target", "comparator": comparator}
    )
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["a", "btt", "lll"]},
            "VAR2",
            [False, False, True],
        ),
        (
            {"target": [["A", "B", "C"], ["A", "B", "L"], ["L", "Q", "R"]]},
            "l",
            [True, False, False],
        ),
    ],
)
def test_does_not_contain_case_insensitive(data, comparator, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.does_not_contain_case_insensitive(
        {"target": "target", "comparator": comparator}
    )
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        ({"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "Btt", "A"]}, "VAR2", True),
        ({"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "Btt", "D"]}, "VAR2", False),
    ],
)
def test_contains_all(data, comparator, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.contains_all({"target": "target", "comparator": comparator})
    assert result is expected_result


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        ({"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "Btt", "A"]}, "VAR2", False),
        ({"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "Btt", "D"]}, "VAR2", True),
    ],
)
def test_not_contains_all(data, comparator, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.not_contains_all(
        {"target": "target", "comparator": comparator}
    )
    assert result is expected_result
