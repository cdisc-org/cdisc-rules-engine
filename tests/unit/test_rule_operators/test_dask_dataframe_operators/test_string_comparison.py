from cdisc_rules_engine.rule_operators.dask_dataframe_operators import DaskDataframeType
import dask.dataframe as dd
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "data,comparator,regex,expected_result",
    [
        (
            {"VAR2": ["blaAtt", "yyyBtt", "aaaCtt"], "target": ["A", "B", "D"]},
            "VAR2",
            ".{3}(.).*",
            [True, True, False],
        ),
    ],
)
def test_equals_string_part(data, comparator, regex, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.equals_string_part(
        {"target": "target", "comparator": comparator, "regex": regex}
    )
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "B", "D"]},
            "VAR2",
            [True, True, False],
        ),
    ],
)
def test_starts_with(data, comparator, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.starts_with({"target": "target", "comparator": comparator})
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["tt", "B", "D"]},
            "VAR2",
            [True, True, True],
        ),
    ],
)
def test_ends_with(data, comparator, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.ends_with({"target": "target", "comparator": comparator})
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "Bd", "lll"]},
            "VAR2",
            [False, False, True],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "Bd", "lll"]},
            3,
            [True, True, True],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": [2, 3, 2]},
            "VAR2",
            [False, True, False],
        ),
    ],
)
def test_has_equal_length(data, comparator, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.has_equal_length(
        {"target": "target", "comparator": comparator}
    )
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "Bd", "lll"]},
            "VAR2",
            [True, True, False],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "Bd", "lll"]},
            3,
            [False, False, False],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": [2, 3, 2]},
            "VAR2",
            [True, False, True],
        ),
    ],
)
def test_has_not_equal_length(data, comparator, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.has_not_equal_length(
        {"target": "target", "comparator": comparator}
    )
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "Bd", "lll"]},
            "VAR2",
            [True, True, False],
        ),
        (
            {"target": ["Att", "Btt", "Ctta"], "VAR2": ["A", "Bd", "lll"]},
            3,
            [False, False, True],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": [2, 3, 2]},
            "VAR2",
            [True, False, True],
        ),
    ],
)
def test_longer_than(data, comparator, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.longer_than({"target": "target", "comparator": comparator})
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["AiAa", "Bd", "lll"]},
            "VAR2",
            [False, True, True],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "Bd", "lll"]},
            3,
            [True, True, True],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": [2, 3, 2]},
            "VAR2",
            [True, True, True],
        ),
    ],
)
def test_longer_than_or_equal_to(data, comparator, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.longer_than_or_equal_to(
        {"target": "target", "comparator": comparator}
    )
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "Bd", "lll"]},
            "VAR2",
            [False, False, False],
        ),
        (
            {"target": ["At", "Btt", "Ctta"], "VAR2": ["A", "Bd", "lll"]},
            3,
            [True, False, False],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": [2, 5, 2]},
            "VAR2",
            [False, True, False],
        ),
    ],
)
def test_shorter_than(data, comparator, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.shorter_than({"target": "target", "comparator": comparator})
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["AiAa", "Bd", "lll"]},
            "VAR2",
            [True, False, True],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "Bd", "lll"]},
            3,
            [True, True, True],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": [2, 3, 2]},
            "VAR2",
            [False, True, False],
        ),
    ],
)
def test_shorter_than_or_equal_to(data, comparator, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.shorter_than_or_equal_to(
        {"target": "target", "comparator": comparator}
    )
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,expected_result",
    [
        ({"target": ["Att", "", None]}, [False, True, True]),
    ],
)
def test_empty(data, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.empty({"target": "target"})
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,expected_result",
    [
        ({"target": ["Att", "", None]}, [True, False, False]),
    ],
)
def test_non_empty(data, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.non_empty({"target": "target"})
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,prefix,expected_result",
    [
        (
            {
                "target": ["word", "TEST"],
            },
            "w.*",
            2,
            [True, False],
        ),
        (
            {
                "target": ["word", "TEST"],
            },
            "[0-9].*",
            2,
            [False, False],
        ),
    ],
)
def test_prefix_matches_regex(data, comparator, prefix, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.prefix_matches_regex(
        {"target": "target", "comparator": comparator, "prefix": prefix}
    )
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,suffix,expected_result",
    [
        (
            {
                "target": ["WORD", "test"],
            },
            "es.*",
            3,
            [False, True],
        ),
        (
            {
                "target": ["word", "TEST"],
            },
            "[0-9].*",
            3,
            [False, False],
        ),
    ],
)
def test_suffix_matches_regex(data, comparator, suffix, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.suffix_matches_regex(
        {"target": "target", "comparator": comparator, "suffix": suffix}
    )
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,suffix,expected_result",
    [
        (
            {
                "target": ["WORD", "test"],
            },
            ".*",
            3,
            [False, False],
        ),
        (
            {
                "target": ["word", "TEST"],
            },
            "[0-9].*",
            3,
            [True, True],
        ),
    ],
)
def test_not_suffix_matches_regex(data, comparator, suffix, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.not_suffix_matches_regex(
        {"target": "target", "comparator": comparator, "suffix": suffix}
    )
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,prefix,expected_result",
    [
        (
            {
                "target": ["word", "TEST"],
            },
            ".*",
            2,
            [False, False],
        ),
        (
            {
                "target": ["word", "TEST"],
            },
            "[0-9].*",
            2,
            [True, True],
        ),
    ],
)
def test_not_prefix_matches_regex(data, comparator, prefix, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.not_prefix_matches_regex(
        {"target": "target", "comparator": comparator, "prefix": prefix}
    )
    assert result.compute().equals(pd.Series(expected_result))
