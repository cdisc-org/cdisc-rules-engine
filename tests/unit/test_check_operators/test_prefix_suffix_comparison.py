from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
import pytest
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


@pytest.mark.parametrize(
    "data,comparator,suffix,dataset_type,expected_result",
    [
        (
            {"target": ["Atest", "B", "Ctest"], "VAR2": ["test", "test", "test"]},
            "VAR2",
            4,
            PandasDataset,
            [True, False, True],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            1,
            PandasDataset,
            [False, True, False],
        ),
        (
            {"target": ["Atest", "B", "Ctest"], "VAR2": ["test", "test", "test"]},
            "VAR2",
            4,
            DaskDataset,
            [True, False, True],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            1,
            DaskDataset,
            [False, True, False],
        ),
    ],
)
def test_suffix_equal_to(data, comparator, suffix, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.suffix_equal_to(
        {"target": "target", "comparator": comparator, "suffix": suffix}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,suffix,dataset_type,expected_result",
    [
        (
            {"target": ["Atest", "B", "Ctest"], "VAR2": ["test", "test", "test"]},
            "VAR2",
            4,
            PandasDataset,
            [False, True, False],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            1,
            DaskDataset,
            [True, False, True],
        ),
    ],
)
def test_suffix_not_equal_to(data, comparator, suffix, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.suffix_not_equal_to(
        {"target": "target", "comparator": comparator, "suffix": suffix}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,prefix,dataset_type,expected_result",
    [
        (
            {"target": ["testA", "B", "testC"], "VAR2": ["test", "test", "test"]},
            "VAR2",
            4,
            PandasDataset,
            [True, False, True],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            1,
            DaskDataset,
            [False, True, False],
        ),
    ],
)
def test_prefix_equal_to(data, comparator, prefix, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.prefix_equal_to(
        {"target": "target", "comparator": comparator, "prefix": prefix}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,prefix,dataset_type,expected_result",
    [
        (
            {"target": ["testA", "B", "testC"], "VAR2": ["test", "test", "test"]},
            "VAR2",
            4,
            PandasDataset,
            [False, True, False],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            1,
            DaskDataset,
            [True, False, True],
        ),
    ],
)
def test_prefix_not_equal_to(data, comparator, prefix, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.prefix_not_equal_to(
        {"target": "target", "comparator": comparator, "prefix": prefix}
    )
    assert result.equals(df.convert_to_series(expected_result))
