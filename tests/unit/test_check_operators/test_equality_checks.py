from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
import pytest
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset

from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "VAR2",
            PandasDataset,
            [True, True, True],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            DaskDataset,
            [False, True, False],
        ),
    ],
)
def test_equal_to(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.equal_to({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["A", "B", ""], "VAR2": ["", "", ""]},
            "VAR2",
            PandasDataset,
            [False, False, False],
        ),
        (
            {"target": ["A", "B", None], "VAR2": ["A", "B", "C"]},
            "",
            DaskDataset,
            [False, False, False],
        ),
    ],
)
def test_equal_to_null_strings(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.equal_to({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "VAR2",
            PandasDataset,
            [False, False, False],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            DaskDataset,
            [True, False, True],
        ),
    ],
)
def test_not_equal_to(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.not_equal_to({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["A", "B", "C"], "VAR2": ["a", "b", "c"]},
            "VAR2",
            PandasDataset,
            [True, True, True],
        ),
        (
            {"target": ["A", "b", "B"], "VAR2": ["A", "B", "C"]},
            "B",
            DaskDataset,
            [False, True, True],
        ),
    ],
)
def test_equal_to_case_insensitive(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.equal_to_case_insensitive(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["A", "B", "C"], "VAR2": ["a", "b", "c"]},
            "VAR2",
            PandasDataset,
            [False, False, False],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "b",
            DaskDataset,
            [True, False, True],
        ),
    ],
)
def test_not_equal_to_case_insensitive(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.not_equal_to_case_insensitive(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))
