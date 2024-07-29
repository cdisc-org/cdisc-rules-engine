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
        ("invalid", PandasDataset, [False, False, False]),
        ("invalid", DaskDataset, [False, False, False]),
    ],
)
def test_exists(target, dataset_type, expected_result):
    data = {
        "var1": [
            1,
            2,
            4,
        ],
        "var2": [
            3,
            5,
            6,
        ],
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
        ("invalid", PandasDataset, [True, True, True]),
        ("invalid", DaskDataset, [True, True, True]),
    ],
)
def test_not_exists(target, dataset_type, expected_result):
    data = {
        "var1": [
            1,
            2,
            4,
        ],
        "var2": [
            3,
            5,
            6,
        ],
    }
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df, "column_prefix_map": {"--": "va"}}).not_exists(
        {"target": target}
    )
    assert result.equals(df.convert_to_series(expected_result))
