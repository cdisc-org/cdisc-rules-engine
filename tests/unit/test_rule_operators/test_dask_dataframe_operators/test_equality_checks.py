from cdisc_rules_engine.rule_operators.dask_dataframe_operators import DaskDataframeType
import dask as dd
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "VAR2",
            [True, True, True],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            [False, True, False],
        ),
    ],
)
def test_equal_to(data, comparator, expected_result):
    df = dd.dataframe.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.equal_to({"target": "target", "comparator": comparator})
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["A", "B", ""], "VAR2": ["", "", ""]},
            "VAR2",
            [False, False, False],
        ),
        (
            {"target": ["A", "B", None], "VAR2": ["A", "B", "C"]},
            "",
            [False, False, False],
        ),
    ],
)
def test_equal_to_null_strings(data, comparator, expected_result):
    df = dd.dataframe.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.equal_to({"target": "target", "comparator": comparator})
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "VAR2",
            [False, False, False],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            [True, False, True],
        ),
    ],
)
def test_not_equal_to(data, comparator, expected_result):
    df = dd.dataframe.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.not_equal_to({"target": "target", "comparator": comparator})
    assert result.compute().equals(pd.Series(expected_result))


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
    ],
)
def test_equal_to_case_insensitive(data, comparator, expected_result):
    df = dd.dataframe.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.equal_to_case_insensitive(
        {"target": "target", "comparator": comparator}
    )
    assert result.compute().equals(pd.Series(expected_result))


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
    ],
)
def test_not_equal_to_case_insensitive(data, comparator, expected_result):
    df = dd.dataframe.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.not_equal_to_case_insensitive(
        {"target": "target", "comparator": comparator}
    )
    assert result.compute().equals(pd.Series(expected_result))