from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
import pytest
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


@pytest.mark.parametrize(
    "target, dataset_type, expected_result",
    [
        ("var1", PandasDataset, [True, True, True]),
        ("var1", DaskDataset, [True, True, True]),
        ("--r1", PandasDataset, [True, True, True]),
        ("--r1", DaskDataset, [True, True, True]),
        ("nested_var", PandasDataset, [True, True, True]),
        ("nested_var", DaskDataset, [True, True, True]),
        ("invalid", PandasDataset, [False, False, False]),
        ("invalid", DaskDataset, [False, False, False]),
        ("a", PandasDataset, [True, True, True]),
        ("a", DaskDataset, [True, True, True]),
        ("f", PandasDataset, [True, True, True]),
        ("f", DaskDataset, [True, True, True]),
        ("x", PandasDataset, [False, False, False]),
        ("x", DaskDataset, [False, False, False]),
        ("non_nested_value", PandasDataset, [True, True, True]),
        ("non_nested_value", DaskDataset, [True, True, True]),
    ],
)
def test_exists(target, dataset_type, expected_result):
    data = {
        "var1": [1, 2, 4],
        "var2": [3, 5, 6],
        "nested_var": [["a", "b", "c"], ["d", "e"], ["f", "nested_var", "g"]],
        "non_nested_value": ["h", "i", "j"],
    }
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df, "column_prefix_map": {"--": "va"}}).exists(
        {"target": target}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "target, dataset_type, expected_result",
    [
        ("var1", PandasDataset, [False, False, False]),
        ("var1", DaskDataset, [False, False, False]),
        ("--r1", PandasDataset, [False, False, False]),
        ("--r1", DaskDataset, [False, False, False]),
        ("nested_var", PandasDataset, [False, False, False]),
        ("nested_var", DaskDataset, [False, False, False]),
        ("invalid", PandasDataset, [True, True, True]),
        ("invalid", DaskDataset, [True, True, True]),
        ("a", PandasDataset, [False, False, False]),
        ("a", DaskDataset, [False, False, False]),
        ("f", PandasDataset, [False, False, False]),
        ("f", DaskDataset, [False, False, False]),
        ("x", PandasDataset, [True, True, True]),
        ("x", DaskDataset, [True, True, True]),
        ("non_nested_value", PandasDataset, [False, False, False]),
        ("non_nested_value", DaskDataset, [False, False, False]),
    ],
)
def test_not_exists(target, dataset_type, expected_result):
    data = {
        "var1": [1, 2, 4],
        "var2": [3, 5, 6],
        "nested_var": [["a", "b", "c"], ["d", "e"], ["f", "nested_var", "g"]],
        "non_nested_value": ["h", "i", "j"],
    }
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df, "column_prefix_map": {"--": "va"}}).not_exists(
        {"target": target}
    )
    assert result.equals(df.convert_to_series(expected_result))
