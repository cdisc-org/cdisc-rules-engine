from cdisc_rules_engine.rule_operators.dataframe_operators import DataframeType
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "data,comparator,suffix,expected_result",
    [
        (
            {"target": ["Atest", "B", "Ctest"], "VAR2": ["test", "test", "test"]},
            "VAR2",
            4,
            [True, False, True],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            1,
            [False, True, False],
        ),
    ],
)
def test_suffix_equal_to(data, comparator, suffix, expected_result):
    df = pd.DataFrame.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.suffix_equal_to(
        {"target": "target", "comparator": comparator, "suffix": suffix}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,suffix,expected_result",
    [
        (
            {"target": ["Atest", "B", "Ctest"], "VAR2": ["test", "test", "test"]},
            "VAR2",
            4,
            [False, True, False],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            1,
            [True, False, True],
        ),
    ],
)
def test_suffix_not_equal_to(data, comparator, suffix, expected_result):
    df = pd.DataFrame.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.suffix_not_equal_to(
        {"target": "target", "comparator": comparator, "suffix": suffix}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,prefix,expected_result",
    [
        (
            {"target": ["testA", "B", "testC"], "VAR2": ["test", "test", "test"]},
            "VAR2",
            4,
            [True, False, True],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            1,
            [False, True, False],
        ),
    ],
)
def test_prefix_equal_to(data, comparator, prefix, expected_result):
    df = pd.DataFrame.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.prefix_equal_to(
        {"target": "target", "comparator": comparator, "prefix": prefix}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,prefix,expected_result",
    [
        (
            {"target": ["testA", "B", "testC"], "VAR2": ["test", "test", "test"]},
            "VAR2",
            4,
            [False, True, False],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            1,
            [True, False, True],
        ),
    ],
)
def test_prefix_not_equal_to(data, comparator, prefix, expected_result):
    df = pd.DataFrame.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.prefix_not_equal_to(
        {"target": "target", "comparator": comparator, "prefix": prefix}
    )
    assert result.equals(pd.Series(expected_result))
