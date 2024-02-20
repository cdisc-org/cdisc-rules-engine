from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
import pytest
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": [1, 2, 3], "VAR2": [3, 3, 3]},
            "VAR2",
            PandasDataset,
            [True, True, False],
        ),
        (
            {"target": [1, 2, 3], "VAR2": [3, 3, 3]},
            2,
            DaskDataset,
            [True, False, False],
        ),
        (
            {"target": ["1", "2", "3"], "VAR2": ["3", "3", "3"]},
            "VAR2",
            PandasDataset,
            [True, True, False],
        ),
    ],
)
def test_less_than(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.less_than({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": [1, 2, 3], "VAR2": [3, 3, 3]},
            "VAR2",
            PandasDataset,
            [True, True, True],
        ),
        ({"target": [1, 2, 3], "VAR2": [3, 3, 3]}, 2, DaskDataset, [True, True, False]),
        (
            {"target": ["1", "2", "3"], "VAR2": ["3", "3", "3"]},
            "VAR2",
            PandasDataset,
            [True, True, True],
        ),
    ],
)
def test_less_than_or_equal_to(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.less_than_or_equal_to(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": [1, 2, 4], "VAR2": [3, 3, 3]},
            "VAR2",
            PandasDataset,
            [False, False, True],
        ),
        (
            {"target": [1, 2, 3], "VAR2": [3, 3, 3]},
            2,
            DaskDataset,
            [False, False, True],
        ),
        (
            {"target": ["1", "2", "3"], "VAR2": ["3", "3", "3"]},
            "VAR2",
            PandasDataset,
            [False, False, False],
        ),
    ],
)
def test_greater_than(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.greater_than({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": [1, 2, 3], "VAR2": [3, 3, 3]},
            "VAR2",
            PandasDataset,
            [False, False, True],
        ),
        ({"target": [1, 2, 3], "VAR2": [3, 3, 3]}, 2, DaskDataset, [False, True, True]),
        (
            {"target": ["1", "2", "3"], "VAR2": ["3", "3", "3"]},
            "VAR2",
            PandasDataset,
            [False, False, True],
        ),
    ],
)
def test_greater_than_or_equal_to(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.greater_than_or_equal_to(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))
