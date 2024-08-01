from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
import pytest
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


@pytest.mark.parametrize(
    "dataset_type, target, prefix_length, expected_result",
    [
        (PandasDataset, "var1", 2, [True, True, True]),
        (DaskDataset, "var1", 2, [True, True, True]),
        (PandasDataset, "var2", 2, [True, False, False]),
        (DaskDataset, "var2", 2, [True, False, False]),
    ],
)
def test_prefix_is_contained_by(dataset_type, target, prefix_length, expected_result):
    data = {
        "var1": ["AETEST", "AETESTCD", "LBTEST"],
        "var2": ["AETEST", "AFTESTCD", "RRTEST"],
        "study_domains": [
            ["DM", "AE", "LB", "TV"],
            ["DM", "AE", "LB", "TV"],
            ["DM", "AE", "LB", "TV"],
        ],
    }
    df = dataset_type.from_dict(data)
    assert (
        DataframeType({"value": df})
        .prefix_is_contained_by(
            {
                "target": target,
                "comparator": "study_domains",
                "prefix": prefix_length,
            }
        )
        .equals(df.convert_to_series(expected_result))
    )


@pytest.mark.parametrize(
    "dataset_type, target, prefix_length, expected_result",
    [
        (PandasDataset, "var1", 2, [False, False, False]),
        (DaskDataset, "var1", 2, [False, False, False]),
        (PandasDataset, "var2", 2, [False, True, True]),
        (DaskDataset, "var2", 2, [False, True, True]),
    ],
)
def test_prefix_is_not_contained_by(
    dataset_type, target, prefix_length, expected_result
):
    data = {
        "var1": ["AETEST", "AETESTCD", "LBTEST"],
        "var2": ["AETEST", "AFTESTCD", "RRTEST"],
        "study_domains": [
            ["DM", "AE", "LB", "TV"],
            ["DM", "AE", "LB", "TV"],
            ["DM", "AE", "LB", "TV"],
        ],
    }
    df = dataset_type.from_dict(data)
    assert (
        DataframeType({"value": df})
        .prefix_is_not_contained_by(
            {
                "target": target,
                "comparator": "study_domains",
                "prefix": prefix_length,
            }
        )
        .equals(df.convert_to_series(expected_result))
    )


@pytest.mark.parametrize(
    "dataset_type, target, suffix_length, expected_result",
    [
        (PandasDataset, "var1", 2, [True, True, True]),
        (DaskDataset, "var1", 2, [True, True, True]),
        (PandasDataset, "var2", 2, [True, True, False]),
        (DaskDataset, "var2", 2, [True, True, False]),
    ],
)
def test_suffix_is_contained_by(dataset_type, target, suffix_length, expected_result):
    data = {
        "var1": ["AETEST", "AETESTCD", "LBTEGG"],
        "var2": ["AETEST", "AFTESTCD", "RRTELE"],
        "study_domains": [
            ["ST", "CD", "GG", "TV"],
            ["ST", "CD", "GG", "TV"],
            ["ST", "CD", "GG", "TV"],
        ],
    }
    df = dataset_type.from_dict(data)
    assert (
        DataframeType({"value": df})
        .suffix_is_contained_by(
            {
                "target": target,
                "comparator": "study_domains",
                "suffix": suffix_length,
            }
        )
        .equals(df.convert_to_series(expected_result))
    )


@pytest.mark.parametrize(
    "dataset_type, target, suffix_length, expected_result",
    [
        (PandasDataset, "var1", 2, [False, False, False]),
        (DaskDataset, "var1", 2, [False, False, False]),
        (PandasDataset, "var2", 2, [False, False, True]),
        (DaskDataset, "var2", 2, [False, False, True]),
    ],
)
def test_suffix_is_not_contained_by(
    dataset_type, target, suffix_length, expected_result
):
    data = {
        "var1": ["AETEST", "AETESTCD", "LBTEGG"],
        "var2": ["AETEST", "AFTESTCD", "RRTELE"],
        "study_domains": [
            ["ST", "CD", "GG", "TV"],
            ["ST", "CD", "GG", "TV"],
            ["ST", "CD", "GG", "TV"],
        ],
    }
    df = dataset_type.from_dict(data)
    assert (
        DataframeType({"value": df})
        .suffix_is_not_contained_by(
            {
                "target": target,
                "comparator": "study_domains",
                "suffix": suffix_length,
            }
        )
        .equals(df.convert_to_series(expected_result))
    )
