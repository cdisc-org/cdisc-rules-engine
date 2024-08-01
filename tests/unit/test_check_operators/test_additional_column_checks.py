from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
import pytest
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


@pytest.mark.parametrize(
    "dataset_type, data, expected_result",
    [
        (
            PandasDataset,
            {
                "USUBJID": [
                    1,
                    1,
                    1,
                    1,
                ],
                "TSVAL": [
                    None,
                    None,
                    "another value",
                    None,
                ],  # original column may be empty
                "TSVAL1": [
                    "value",
                    "value",
                    "value",
                    None,
                ],  # valid since TSVAL2 is also null in the same row
                "TSVAL2": [None, "value 2", "value 2", None],
            },
            [False, False, False, False],
        ),
        (
            DaskDataset,
            {
                "USUBJID": [
                    1,
                    1,
                    1,
                    1,
                ],
                "TSVAL": [
                    None,
                    None,
                    "another value",
                    None,
                ],  # original column may be empty
                "TSVAL1": [
                    "value",
                    "value",
                    "value",
                    None,
                ],  # valid since TSVAL2 is also null in the same row
                "TSVAL2": [None, "value 2", "value 2", None],
            },
            [False, False, False, False],
        ),
        (
            PandasDataset,
            {
                "USUBJID": [
                    1,
                    1,
                    1,
                    1,
                ],
                "TSVAL": [
                    "value",
                    None,
                    "another value",
                    None,
                ],  # original column may be empty
                "TSVAL1": ["value", None, "value", "value"],  # invalid column
                "TSVAL2": ["value 2", "value 2", "value 2", None],
                "TSVAL3": ["value 3", "value 3", None, "value 3"],
            },
            [False, True, False, True],
        ),
        (
            DaskDataset,
            {
                "USUBJID": [
                    1,
                    1,
                    1,
                    1,
                ],
                "TSVAL": [
                    "value",
                    None,
                    "another value",
                    None,
                ],  # original column may be empty
                "TSVAL1": ["value", None, "value", "value"],  # invalid column
                "TSVAL2": ["value 2", "value 2", "value 2", None],
                "TSVAL3": ["value 3", "value 3", None, "value 3"],
            },
            [False, True, False, True],
        ),
    ],
)
def test_additional_columns_empty(dataset_type, data, expected_result):
    """
    Unit test for additional_columns_empty operator.
    """
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df}).additional_columns_empty(
        {
            "target": "TSVAL",
        }
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "dataset_type, data, expected_result",
    [
        (
            PandasDataset,
            {
                "USUBJID": [
                    1,
                    1,
                    1,
                    1,
                ],
                "TSVAL": [
                    "value",
                    None,
                    "another value",
                    None,
                ],  # original column may be empty
                "TSVAL1": ["value", None, "value", "value"],
                "TSVAL2": ["value 2", "value 2", "value 2", "value 2"],
            },
            [
                True,
                False,
                True,
                True,
            ],
        ),
        (
            DaskDataset,
            {
                "USUBJID": [
                    1,
                    1,
                    1,
                    1,
                ],
                "TSVAL": [
                    "value",
                    None,
                    "another value",
                    None,
                ],  # original column may be empty
                "TSVAL1": ["value", None, "value", "value"],
                "TSVAL2": ["value 2", "value 2", "value 2", "value 2"],
            },
            [
                True,
                False,
                True,
                True,
            ],
        ),
        (
            PandasDataset,
            {
                "USUBJID": [
                    1,
                    1,
                    1,
                    1,
                ],
                "TSVAL": [
                    "value",
                    None,
                    "another value",
                    None,
                ],  # original column may be empty
                "TSVAL1": ["value", "value", "value", "value"],
                "TSVAL2": ["value 2", "value 2", "value 2", "value 2"],
            },
            [
                True,
                True,
                True,
                True,
            ],
        ),
        (
            DaskDataset,
            {
                "USUBJID": [
                    1,
                    1,
                    1,
                    1,
                ],
                "TSVAL": [
                    "value",
                    None,
                    "another value",
                    None,
                ],  # original column may be empty
                "TSVAL1": ["value", "value", "value", "value"],
                "TSVAL2": ["value 2", "value 2", "value 2", "value 2"],
            },
            [
                True,
                True,
                True,
                True,
            ],
        ),
    ],
)
def test_additional_columns_not_empty(dataset_type, data, expected_result):
    """
    Unit test for additional_columns_empty operator.
    """
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df}).additional_columns_not_empty(
        {
            "target": "TSVAL",
        }
    )
    assert result.equals(df.convert_to_series(expected_result))
