from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
import pytest

from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset


@pytest.mark.parametrize(
    "data,comparator,dataset_type, expected_result",
    [
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "btt", "lll"]},
            "VAR2",
            PandasDataset,
            [True, False, False],
        ),
        (
            {"target": [["A", "B", "C"], ["A", "B", "L"], ["L", "Q", "R"]]},
            "L",
            DaskDataset,
            [False, True, True],
        ),
    ],
)
def test_contains(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_operator = DataframeType({"value": df})
    result = dataframe_operator.contains({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["a", "btt", "lll"]},
            "VAR2",
            DaskDataset,
            [True, True, False],
        ),
        (
            {"target": [["A", "B", "C"], ["A", "B", "L"], ["L", "Q", "R"]]},
            "l",
            PandasDataset,
            [False, True, True],
        ),
    ],
)
def test_contains_case_insensitive(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_operator = DataframeType({"value": df})
    result = dataframe_operator.contains_case_insensitive(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "btt", "lll"]},
            "VAR2",
            PandasDataset,
            [False, True, True],
        ),
        (
            {"target": [["A", "B", "C"], ["A", "B", "L"], ["L", "Q", "R"]]},
            "L",
            DaskDataset,
            [True, False, False],
        ),
    ],
)
def test_does_not_contain(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_operator = DataframeType({"value": df})
    result = dataframe_operator.does_not_contain(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["a", "btt", "lll"]},
            "VAR2",
            DaskDataset,
            [False, False, True],
        ),
        (
            {"target": [["A", "B", "C"], ["A", "B", "L"], ["L", "Q", "R"]]},
            "l",
            PandasDataset,
            [True, False, False],
        ),
    ],
)
def test_does_not_contain_case_insensitive(
    data, comparator, dataset_type, expected_result
):
    df = dataset_type.from_dict(data)
    dataframe_operator = DataframeType({"value": df})
    result = dataframe_operator.does_not_contain_case_insensitive(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "Btt", "A"]},
            "VAR2",
            PandasDataset,
            True,
        ),
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "Btt", "D"]},
            "VAR2",
            DaskDataset,
            False,
        ),
    ],
)
def test_contains_all(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_operator = DataframeType({"value": df})
    result = dataframe_operator.contains_all(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type, expected_result",
    [
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "Btt", "A"]},
            "VAR2",
            DaskDataset,
            False,
        ),
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "Btt", "D"]},
            "VAR2",
            PandasDataset,
            True,
        ),
    ],
)
def test_not_contains_all(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_operator = DataframeType({"value": df})
    result = dataframe_operator.not_contains_all(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))
