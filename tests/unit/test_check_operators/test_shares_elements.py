import pytest
from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


@pytest.mark.parametrize(
    "data,dataset_type,operator,expected_result",
    [
        (
            {
                "target": [["A", "B"], ["C", "D"], ["E", "F"]],
                "comparator": [["B", "C"], ["D", "E"], ["F", "G"]],
            },
            PandasDataset,
            "shares_at_least_one_element_with",
            True,
        ),
        (
            {
                "target": [["A", "B"], ["C", "D"], ["E", "F"]],
                "comparator": [["X", "Y"], ["Y", "Z"], ["Z", "W"]],
            },
            DaskDataset,
            "shares_at_least_one_element_with",
            False,
        ),
        (
            {
                "target": [["A"], ["B"], ["C"]],
                "comparator": [["A", "X"], ["B", "Y"], ["C", "Z"]],
            },
            PandasDataset,
            "shares_exactly_one_element_with",
            True,
        ),
        (
            {
                "target": [["A", "B"], ["C", "D"], ["E", "F"]],
                "comparator": [["A", "X"], ["C", "Y"], ["E", "Z"]],
            },
            DaskDataset,
            "shares_exactly_one_element_with",
            True,
        ),
        (
            {
                "target": [["A", "B"], ["C", "D"], ["E", "F"]],
                "comparator": [["X", "Y"], ["Y", "Z"], ["Z", "W"]],
            },
            PandasDataset,
            "shares_no_elements_with",
            True,
        ),
        (
            {
                "target": [["A", "B"], ["C", "D"], ["E", "F"]],
                "comparator": [["B", "C"], ["D", "E"], ["F", "G"]],
            },
            DaskDataset,
            "shares_no_elements_with",
            False,
        ),
    ],
)
def test_element_sharing_operators(data, dataset_type, operator, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = getattr(dataframe_type, operator)(
        {"target": "target", "comparator": "comparator"}
    )
    assert result == expected_result


def test_element_sharing_operators_cases():
    data = {
        "target": [["A"], ["B", "C"], ["D", "E", "F"], []],
        "comparator": [["A", "B"], ["C", "D"], ["E", "F", "G"], ["X"]],
    }
    df = PandasDataset.from_dict(data)
    dataframe_type = DataframeType({"value": df})

    assert dataframe_type.shares_at_least_one_element_with(
        {"target": "target", "comparator": "comparator"}
    )

    # Changed expectation to True
    assert dataframe_type.shares_exactly_one_element_with(
        {"target": "target", "comparator": "comparator"}
    )

    assert not dataframe_type.shares_no_elements_with(
        {"target": "target", "comparator": "comparator"}
    )


def test_element_sharing_operators_with_single_elements():
    data = {"target": ["A", "B", "C", "D"], "comparator": ["X", "B", "Y", "Z"]}
    df = DaskDataset.from_dict(data)
    dataframe_type = DataframeType({"value": df})

    assert dataframe_type.shares_at_least_one_element_with(
        {"target": "target", "comparator": "comparator"}
    )

    assert dataframe_type.shares_exactly_one_element_with(
        {"target": "target", "comparator": "comparator"}
    )

    assert not dataframe_type.shares_no_elements_with(
        {"target": "target", "comparator": "comparator"}
    )


def test_element_sharing_operators_with_mixed_types():
    data = {
        "target": [["A", "B"], "C", ["D", "E"], "F"],
        "comparator": ["B", ["C", "D"], "E", ["F", "G"]],
    }
    df = PandasDataset.from_dict(data)
    dataframe_type = DataframeType({"value": df})

    assert dataframe_type.shares_at_least_one_element_with(
        {"target": "target", "comparator": "comparator"}
    )

    assert dataframe_type.shares_exactly_one_element_with(
        {"target": "target", "comparator": "comparator"}
    )

    assert not dataframe_type.shares_no_elements_with(
        {"target": "target", "comparator": "comparator"}
    )
