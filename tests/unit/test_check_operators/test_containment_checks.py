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


@pytest.mark.parametrize(
    "data,comparator,dataset_type, expected_result",
    [
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "btt", "lll"]},
            ["Ctt", "B", "A"],
            PandasDataset,
            [True, False, True],
        ),
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "btt", "lll"]},
            ["Ctt", "B", "A"],
            DaskDataset,
            [True, False, True],
        ),
        (
            {"target": ["A", "B", "C"]},
            ["C", "Z", "A"],
            DaskDataset,
            [True, False, True],
        ),
        (
            {"target": [1, 2, 3], "VAR2": [[1, 2], [3], [3]]},
            "VAR2",
            PandasDataset,
            [True, False, True],
        ),
        (
            {"target": [1, 2, 3], "VAR2": [[1, 2], [3], [3]]},
            "VAR2",
            DaskDataset,
            [True, False, True],
        ),
    ],
)
def test_is_contained_by(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_operator = DataframeType({"value": df})
    result = dataframe_operator.is_contained_by(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type, expected_result",
    [
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "btt", "lll"]},
            ["Ctt", "B", "A"],
            PandasDataset,
            [False, True, False],
        ),
        (
            {"target": ["A", "B", "C"]},
            ["C", "Z", "A"],
            DaskDataset,
            [False, True, False],
        ),
        (
            {"target": [1, 2, 3], "VAR2": [[1, 2], [2], [2]]},
            "VAR2",
            PandasDataset,
            [False, False, True],
        ),
        (
            {"target": [1, 2, 3], "VAR2": [[1, 2], [2], [2]]},
            "VAR2",
            DaskDataset,
            [False, False, True],
        ),
    ],
)
def test_is_not_contained_by(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_operator = DataframeType({"value": df})
    result = dataframe_operator.is_not_contained_by(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type, expected_result",
    [
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "btt", "lll"]},
            ["ctt", "b", "a"],
            PandasDataset,
            [True, False, True],
        ),
        (
            {"target": ["A", "B", "C"]},
            ["c", "z", "a"],
            DaskDataset,
            [True, False, True],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": [["a", "b"], ["c"], ["c"]]},
            "VAR2",
            PandasDataset,
            [True, False, True],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": [["a", "b"], ["c"], ["c"]]},
            "VAR2",
            DaskDataset,
            [True, False, True],
        ),
    ],
)
def test_is_contained_by_case_insensitive(
    data, comparator, dataset_type, expected_result
):
    df = dataset_type.from_dict(data)
    dataframe_operator = DataframeType({"value": df})
    result = dataframe_operator.is_contained_by_case_insensitive(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type, expected_result",
    [
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "btt", "lll"]},
            ["ctt", "b", "a"],
            PandasDataset,
            [False, True, False],
        ),
        (
            {"target": ["A", "B", "C"]},
            ["c", "z", "a"],
            DaskDataset,
            [False, True, False],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": [["a", "b"], ["b"], ["b"]]},
            "VAR2",
            PandasDataset,
            [False, False, True],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": [["a", "b"], ["b"], ["b"]]},
            "VAR2",
            DaskDataset,
            [False, False, True],
        ),
    ],
)
def test_is_not_contained_by_case_insensitive(
    data, comparator, dataset_type, expected_result
):
    df = dataset_type.from_dict(data)
    dataframe_operator = DataframeType({"value": df})
    result = dataframe_operator.is_not_contained_by_case_insensitive(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))
