from cdisc_rules_engine.rule_operators.dask_dataframe_operators import DaskDataframeType
import dask as dd
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        ({"target": [1, 2, 3], "VAR2": [3, 3, 3]}, "VAR2", [True, True, False]),
        ({"target": [1, 2, 3], "VAR2": [3, 3, 3]}, 2, [True, False, False]),
        (
            {"target": ["1", "2", "3"], "VAR2": ["3", "3", "3"]},
            "VAR2",
            [True, True, False],
        ),
    ],
)
def test_less_than(data, comparator, expected_result):
    df = dd.dataframe.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.less_than({"target": "target", "comparator": comparator})
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        ({"target": [1, 2, 3], "VAR2": [3, 3, 3]}, "VAR2", [True, True, True]),
        ({"target": [1, 2, 3], "VAR2": [3, 3, 3]}, 2, [True, True, False]),
        (
            {"target": ["1", "2", "3"], "VAR2": ["3", "3", "3"]},
            "VAR2",
            [True, True, True],
        ),
    ],
)
def test_less_than_or_equal_to(data, comparator, expected_result):
    df = dd.dataframe.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.less_than_or_equal_to(
        {"target": "target", "comparator": comparator}
    )
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        ({"target": [1, 2, 4], "VAR2": [3, 3, 3]}, "VAR2", [False, False, True]),
        ({"target": [1, 2, 3], "VAR2": [3, 3, 3]}, 2, [False, False, True]),
        (
            {"target": ["1", "2", "3"], "VAR2": ["3", "3", "3"]},
            "VAR2",
            [False, False, False],
        ),
    ],
)
def test_greater_than(data, comparator, expected_result):
    df = dd.dataframe.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.greater_than({"target": "target", "comparator": comparator})
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        ({"target": [1, 2, 3], "VAR2": [3, 3, 3]}, "VAR2", [False, False, True]),
        ({"target": [1, 2, 3], "VAR2": [3, 3, 3]}, 2, [False, True, True]),
        (
            {"target": ["1", "2", "3"], "VAR2": ["3", "3", "3"]},
            "VAR2",
            [False, False, True],
        ),
    ],
)
def test_greater_than_or_equal_to(data, comparator, expected_result):
    df = dd.dataframe.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.greater_than_or_equal_to(
        {"target": "target", "comparator": comparator}
    )
    assert result.compute().equals(pd.Series(expected_result))