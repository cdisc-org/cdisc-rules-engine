from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
import pytest
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
import pandas as pd


@pytest.mark.parametrize(
    "data,comparator,within,dataset_type,expected_result",
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
            DaskDataset,
            [True, True, True, False, True, True],
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
                    "AEHOSP3",
                    "AEHOSP2",
                    "AEHOSP2",
                ],
            },
            1,
            "USUBJID",
            PandasDataset,
            [True, True, True, False, True, True],
        ),
    ],
)
def test_present_on_multiple_rows_within(
    data, comparator, within, dataset_type, expected_result
):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.present_on_multiple_rows_within(
        {"target": "target", "comparator": comparator, "within": within}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data, dataset_type, expected_result",
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
            PandasDataset,
            [True, True, True, True, True, True],
        ),
    ],
)
def test_has_different_values(data, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.has_different_values({"target": "target"})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data, dataset_type, expected_result",
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
            PandasDataset,
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
            DaskDataset,
            [True, True, True, True, True, True],
        ),
    ],
)
def test_has_same_values(data, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.has_same_values({"target": "target"})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
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
            PandasDataset,
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
            DaskDataset,
            [True, True, True, False],
        ),
    ],
)
def test_is_not_unique_relationship(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.is_not_unique_relationship(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,context,dataset_type,expected_result",
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
            DaskDataset,
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
            PandasDataset,
            [True, True, False],
        ),
    ],
)
def test_valid_relationship(data, comparator, context, dataset_type, expected_result):
    reference_data = {
        "LB": {
            "TEST": pd.Series([4, 5, 6]).values,
            "DATA": pd.Series([1, 2, 3]).values,
        },
        "AE": {"AETERM": pd.Series([31, 323, 33]).values},
    }
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df, "relationship_data": reference_data})
    result = dataframe_type.is_valid_relationship(
        {"target": "target", "comparator": comparator, "context": context}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,context,dataset_type,expected_result",
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
            PandasDataset,
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
            DaskDataset,
            [False, False, True],
        ),
    ],
)
def test_is_not_valid_relationship(
    data, comparator, context, dataset_type, expected_result
):
    reference_data = {
        "LB": {
            "TEST": pd.Series([4, 5, 6]).values,
            "DATA": pd.Series([1, 2, 3]).values,
        },
        "AE": {"AETERM": pd.Series([31, 323, 33]).values},
    }
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df, "relationship_data": reference_data})
    result = dataframe_type.is_not_valid_relationship(
        {"target": "target", "comparator": comparator, "context": context}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,context,dataset_type,expected_result",
    [
        (
            {
                "RDOMAIN": ["LB", "LB", "AE"],
                "target": ["TEST", "DATA", "AETERM"],
            },
            "RDOMAIN",
            PandasDataset,
            [True, True, True],
        ),
        (
            {"RDOMAIN": ["LB", "LB", "AE"], "target": ["TEST", "AETERM", "AETERM"]},
            "RDOMAIN",
            DaskDataset,
            [True, False, True],
        ),
    ],
)
def test_is_valid_reference(data, context, dataset_type, expected_result):
    reference_data = {
        "LB": {"TEST": [], "DATA": [1, 2, 3]},
        "AE": {"AETERM": [1, 2, 3]},
    }
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df, "relationship_data": reference_data})
    result = dataframe_type.is_valid_reference({"target": "target", "context": context})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,context,dataset_type,expected_result",
    [
        (
            {
                "RDOMAIN": ["LB", "LB", "AE"],
                "target": ["TEST", "DATA", "AETERM"],
            },
            "RDOMAIN",
            DaskDataset,
            [False, False, False],
        ),
        (
            {
                "RDOMAIN": ["LB", "LB", "AE"],
                "target": ["TEST", "AETERM", "AETERM"],
            },
            "RDOMAIN",
            PandasDataset,
            [False, True, False],
        ),
    ],
)
def test_is_not_valid_reference(data, context, dataset_type, expected_result):
    reference_data = {
        "LB": {"TEST": [], "DATA": [1, 2, 3]},
        "AE": {"AETERM": [1, 2, 3]},
    }
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df, "relationship_data": reference_data})
    result = dataframe_type.is_not_valid_reference(
        {"target": "target", "context": context}
    )
    assert result.equals(df.convert_to_series(expected_result))
