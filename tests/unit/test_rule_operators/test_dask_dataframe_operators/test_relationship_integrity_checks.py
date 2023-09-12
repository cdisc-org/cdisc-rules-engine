from cdisc_rules_engine.rule_operators.dask_dataframe_operators import DaskDataframeType
import dask.dataframe as dd
import dask.array as da
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "data,comparator,within,expected_result",
    [
        (
            {
                "USUBJID": [
                    1,
                    1,
                    1,
                    2,
                    2,
                    2,
                ],
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
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.present_on_multiple_rows_within(
        {"target": "target", "comparator": comparator, "within": within}
    )
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data, expected_result",
    [
        (
            {
                "USUBJID": [
                    1,
                    1,
                    1,
                    2,
                    2,
                    2,
                ],
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
            [True, True, True, True, True, True],
        ),
    ],
)
def test_has_different_values(data, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.has_different_values({"target": "target"})
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data, expected_result",
    [
        (
            {
                "USUBJID": [
                    1,
                    1,
                    1,
                    2,
                    2,
                    2,
                ],
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
            [False, False, False, False, False, False],
        ),
        (
            {
                "USUBJID": [
                    1,
                    1,
                    1,
                    2,
                    2,
                    2,
                ],
                "SEQ": [1, 2, 3, 4, 5, 6],
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
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.has_same_values({"target": "target"})
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {
                "STUDYID": [
                    "TEST",
                    "TEST-1",
                    "TEST-2",
                    "TEST-3",
                ],
                "VISITNUM": [1, 2, 1, 3],
                "target": [
                    "Consulting",
                    "Surgery",
                    "Consulting",
                    "Treatment",
                ],
            },
            "VISITNUM",
            [False, False, False, False],
        ),
        (
            {
                "STUDYID": [
                    "TEST",
                    "TEST-1",
                    "TEST-2",
                    "TEST-3",
                ],
                "VISITNUM": [1, 2, 1, 3],
                "target": [
                    "Consulting",
                    "Surgery",
                    "Surgery",
                    "Treatment",
                ],
            },
            "VISITNUM",
            [True, True, True, True],
        ),
    ],
)
def test_is_not_unique_relationship(data, comparator, expected_result):
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType({"value": df})
    result = dataframe_type.is_not_unique_relationship(
        {"target": "target", "comparator": comparator}
    )
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,context,expected_result",
    [
        (
            {
                "RDOMAIN": ["LB", "LB", "AE"],
                "target": ["TEST", "DATA", "AETERM"],
                "IDVARVAL1": [4, 1, 31],
                "IDVARVAL2": [5, 1, 35],
            },
            "IDVARVAL1",
            "RDOMAIN",
            [True, True, True],
        ),
        (
            {
                "RDOMAIN": ["LB", "LB", "AE"],
                "target": ["TEST", "DATA", "AETERM"],
                "IDVARVAL1": [4, 1, 31],
                "IDVARVAL2": [5, 1, 35],
            },
            "IDVARVAL2",
            "RDOMAIN",
            [True, True, False],
        ),
    ],
)
def test_valid_relationship(data, comparator, context, expected_result):
    reference_data = {
        "LB": {
            "TEST": dd.from_array(da.from_array([4, 5, 6])),
            "DATA": dd.from_array(da.from_array([1, 2, 3])),
        },
        "AE": {"AETERM": dd.from_array(da.from_array([31, 323, 33]))},
    }
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType(
        {"value": df, "relationship_data": reference_data}
    )
    result = dataframe_type.is_valid_relationship(
        {"target": "target", "comparator": comparator, "context": context}
    )
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,context,expected_result",
    [
        (
            {
                "RDOMAIN": ["LB", "LB", "AE"],
                "target": ["TEST", "DATA", "AETERM"],
                "IDVARVAL1": [4, 1, 31],
                "IDVARVAL2": [5, 1, 35],
            },
            "IDVARVAL1",
            "RDOMAIN",
            [False, False, False],
        ),
        (
            {
                "RDOMAIN": ["LB", "LB", "AE"],
                "target": ["TEST", "DATA", "AETERM"],
                "IDVARVAL1": [4, 1, 31],
                "IDVARVAL2": [5, 1, 35],
            },
            "IDVARVAL2",
            "RDOMAIN",
            [False, False, True],
        ),
    ],
)
def test_is_not_valid_relationship(data, comparator, context, expected_result):
    reference_data = {
        "LB": {
            "TEST": dd.from_array(da.from_array([4, 5, 6])),
            "DATA": dd.from_array(da.from_array([1, 2, 3])),
        },
        "AE": {"AETERM": dd.from_array(da.from_array([31, 323, 33]))},
    }
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType(
        {"value": df, "relationship_data": reference_data}
    )
    result = dataframe_type.is_not_valid_relationship(
        {"target": "target", "comparator": comparator, "context": context}
    )
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,context,expected_result",
    [
        (
            {
                "RDOMAIN": ["LB", "LB", "AE"],
                "target": ["TEST", "DATA", "AETERM"],
            },
            "RDOMAIN",
            [True, True, True],
        ),
        (
            {"RDOMAIN": ["LB", "LB", "AE"], "target": ["TEST", "AETERM", "AETERM"]},
            "RDOMAIN",
            [True, False, True],
        ),
    ],
)
def test_is_valid_reference(data, context, expected_result):
    reference_data = {
        "LB": {"TEST": [], "DATA": [1, 2, 3]},
        "AE": {"AETERM": [1, 2, 3]},
    }
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType(
        {"value": df, "relationship_data": reference_data}
    )
    result = dataframe_type.is_valid_reference({"target": "target", "context": context})
    assert result.compute().equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,context,expected_result",
    [
        (
            {
                "RDOMAIN": ["LB", "LB", "AE"],
                "target": ["TEST", "DATA", "AETERM"],
            },
            "RDOMAIN",
            [False, False, False],
        ),
        (
            {
                "RDOMAIN": ["LB", "LB", "AE"],
                "target": ["TEST", "AETERM", "AETERM"],
            },
            "RDOMAIN",
            [False, True, False],
        ),
    ],
)
def test_is_not_valid_reference(data, context, expected_result):
    reference_data = {
        "LB": {"TEST": [], "DATA": [1, 2, 3]},
        "AE": {"AETERM": [1, 2, 3]},
    }
    df = dd.from_dict(data, npartitions=1)
    dataframe_type = DaskDataframeType(
        {"value": df, "relationship_data": reference_data}
    )
    result = dataframe_type.is_not_valid_reference(
        {"target": "target", "context": context}
    )
    assert result.compute().equals(pd.Series(expected_result))
