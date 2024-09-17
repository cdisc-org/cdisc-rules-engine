import pytest
import pandas as pd
from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


@pytest.mark.parametrize(
    "dataset_type, data, expected_result",
    [
        (
            PandasDataset,
            {
                "USUBJID": [1, 1, 1, 1],
                "TSVAL": [None, None, "another value", None],
                "TSVAL1": ["value", "value", "value", None],
                "TSVAL2": [None, "value 2", "value 2", None],
            },
            [True, True, False, False],
        ),
        (
            DaskDataset,
            {
                "USUBJID": [1, 1, 1, 1],
                "TSVAL": [None, None, "another value", None],
                "TSVAL1": ["value", "value", "value", None],
                "TSVAL2": [None, "value 2", "value 2", None],
            },
            [True, True, False, False],
        ),
        (
            PandasDataset,
            {
                "USUBJID": [1, 1, 1, 1],
                "TSVAL": ["value", None, "another value", None],
                "TSVAL1": ["value", None, "value", "value"],
                "TSVAL2": ["value 2", "value 2", "value 2", None],
                "TSVAL3": ["value 3", "value 3", None, "value 3"],
            },
            [False, True, False, True],
        ),
        (
            DaskDataset,
            {
                "USUBJID": [1, 1, 1, 1],
                "TSVAL": ["value", None, "another value", None],
                "TSVAL1": ["value", None, "value", "value"],
                "TSVAL2": ["value 2", "value 2", "value 2", None],
                "TSVAL3": ["value 3", "value 3", None, "value 3"],
            },
            [False, True, False, True],
        ),
        (
            PandasDataset,
            {
                "USUBJID": [1, 1, 1, 1],
                "TSVAL": ["value", "value", "value", "value"],
                "TSVAL1": ["value", "value", "value", "value"],
                "TSVAL2": ["value 2", "value 2", "value 2", "value 2"],
            },
            [False, False, False, False],
        ),
        (
            DaskDataset,
            {
                "USUBJID": [1, 1, 1, 1],
                "TSVAL": ["value", "value", "value", "value"],
                "TSVAL1": ["value", "value", "value", "value"],
                "TSVAL2": ["value 2", "value 2", "value 2", "value 2"],
            },
            [False, False, False, False],
        ),
    ],
)
def test_inconsistent_enumerated_columns(dataset_type, data, expected_result):
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df}).inconsistent_enumerated_columns(
        {
            "target": "TSVAL",
        }
    )
    assert (
        result.tolist() == expected_result
    ), f"Expected {expected_result}, got {result.tolist()}"


@pytest.mark.parametrize(
    "dataset_type, data, expected_result",
    [
        (
            PandasDataset,
            {
                "USUBJID": [1],
                "TSVAL": [None],
                "TSVAL1": [None],
                "TSVAL2": ["value"],
            },
            [True],
        ),
        (
            PandasDataset,
            {
                "USUBJID": [1],
                "TSVAL": ["value"],
            },
            [False],
        ),
        (
            PandasDataset,
            {
                "USUBJID": [1],
                "OTHERCOL": ["value"],
            },
            [False],
        ),
    ],
)
def test_inconsistent_enumerated_columns_edge_cases(
    dataset_type, data, expected_result
):
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df}).inconsistent_enumerated_columns(
        {
            "target": "TSVAL",
        }
    )
    assert isinstance(
        result, pd.Series
    ), f"Expected pandas Series result, got {type(result)}"
    assert (
        result.tolist() == expected_result
    ), f"Expected {expected_result}, got {result.tolist()}"
