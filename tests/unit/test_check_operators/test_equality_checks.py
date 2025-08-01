from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
import pytest
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset

from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "VAR2",
            PandasDataset,
            [True, True, True],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            PandasDataset,
            [False, True, False],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "VAR2",
            DaskDataset,
            [True, True, True],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            DaskDataset,
            [False, True, False],
        ),
    ],
)
def test_equal_to(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.equal_to({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["A", "B", ""], "VAR2": ["", "", ""]},
            "VAR2",
            PandasDataset,
            [False, False, False],
        ),
        (
            {"target": ["A", "B", None], "VAR2": ["A", "B", "C"]},
            "",
            DaskDataset,
            [False, False, False],
        ),
    ],
)
def test_equal_to_null_strings(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.equal_to({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,operator,dataset_type,expected_result",
    [
        (
            {
                "IDVARVAL": [320, 2, 15],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": [21, 21, 15],
                "AESEQ": [1, 2, 1],
            },
            "IDVAR",
            "equal_to",
            PandasDataset,
            [False, True, True],
        ),
        (
            {
                "IDVARVAL": [320, 2, 15],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": [21, 21, 15],
                "AESEQ": [1, 2, 1],
            },
            "IDVAR",
            "equal_to",
            DaskDataset,
            [False, True, True],
        ),
        (
            {
                "IDVARVAL": [320, 2, 15],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": [21, 21, 15],
                "AESEQ": [1, 2, 1],
            },
            "IDVAR",
            "not_equal_to",
            PandasDataset,
            [True, False, False],
        ),
        (
            {
                "IDVARVAL": [320, 2, 15],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": [21, 21, 15],
                "AESEQ": [1, 2, 1],
            },
            "IDVAR",
            "not_equal_to",
            DaskDataset,
            [True, False, False],
        ),
    ],
)
def test_equality_operators_value_is_reference(
    data, comparator, operator, dataset_type, expected_result
):
    """Test equal_to and not_equal_to operators with value_is_reference=True for dynamic column comparison."""
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    if operator == "equal_to":
        result = dataframe_type.equal_to(
            {"target": "IDVARVAL", "comparator": comparator, "value_is_reference": True}
        )
    else:
        result = dataframe_type.not_equal_to(
            {"target": "IDVARVAL", "comparator": comparator, "value_is_reference": True}
        )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,operator,dataset_type,expected_result",
    [
        (
            {
                "IDVARVAL": ["320", "2", "15"],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": [320, 21, 15],
                "AESEQ": [1, 2, 1],
            },
            "IDVAR",
            "equal_to",
            PandasDataset,
            [True, True, True],
        ),
        (
            {
                "IDVARVAL": ["320", "2", "15"],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": [320, 21, 15],
                "AESEQ": [1, 2, 1],
            },
            "IDVAR",
            "equal_to",
            DaskDataset,
            [True, True, True],
        ),
        (
            {
                "IDVARVAL": ["320", "2", "15"],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": [320, 21, 15],
                "AESEQ": [1, 2, 1],
            },
            "IDVAR",
            "not_equal_to",
            PandasDataset,
            [False, False, False],
        ),
        (
            {
                "IDVARVAL": ["320", "2", "15"],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": [320, 21, 15],
                "AESEQ": [1, 2, 1],
            },
            "IDVAR",
            "not_equal_to",
            DaskDataset,
            [False, False, False],
        ),
        (
            {
                "IDVARVAL": ["999", "5", "100"],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": [320, 21, 15],
                "AESEQ": [1, 2, 1],
            },
            "IDVAR",
            "equal_to",
            PandasDataset,
            [False, False, False],
        ),
        (
            {
                "IDVARVAL": ["999", "5", "100"],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": [320, 21, 15],
                "AESEQ": [1, 2, 1],
            },
            "IDVAR",
            "not_equal_to",
            DaskDataset,
            [True, True, True],
        ),
        (
            {
                "IDVARVAL": ["1", "2", "3"],
                "IDVAR": ["FLOATCOL", "FLOATCOL", "FLOATCOL"],
                "FLOATCOL": [1.0, 2.0, 3.0],
            },
            "IDVAR",
            "equal_to",
            PandasDataset,
            [True, True, True],
        ),
        (
            {
                "IDVARVAL": ["1", "2", "3"],
                "IDVAR": ["FLOATCOL", "FLOATCOL", "FLOATCOL"],
                "FLOATCOL": [1.0, 2.0, 3.0],
            },
            "IDVAR",
            "equal_to",
            DaskDataset,
            [True, True, True],
        ),
    ],
)
def test_equality_operators_type_insensitive(
    data, comparator, operator, dataset_type, expected_result
):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})

    if operator == "equal_to":
        result = dataframe_type.equal_to(
            {
                "target": "IDVARVAL",
                "comparator": comparator,
                "value_is_reference": True,
                "type_insensitive": True,
            }
        )
    else:
        result = dataframe_type.not_equal_to(
            {
                "target": "IDVARVAL",
                "comparator": comparator,
                "value_is_reference": True,
                "type_insensitive": True,
            }
        )

    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "VAR2",
            PandasDataset,
            [False, False, False],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            DaskDataset,
            [True, False, True],
        ),
    ],
)
def test_not_equal_to(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.not_equal_to({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["A", "B", "C"], "VAR2": ["a", "b", "c"]},
            "VAR2",
            PandasDataset,
            [True, True, True],
        ),
        (
            {"target": ["A", "b", "B"], "VAR2": ["A", "B", "C"]},
            "B",
            DaskDataset,
            [False, True, True],
        ),
    ],
)
def test_equal_to_case_insensitive(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.equal_to_case_insensitive(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["A", "B", "C"], "VAR2": ["a", "b", "c"]},
            "VAR2",
            PandasDataset,
            [False, False, False],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "b",
            DaskDataset,
            [True, False, True],
        ),
    ],
)
def test_not_equal_to_case_insensitive(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.not_equal_to_case_insensitive(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["", "", None], "VAR2": ["", None, ""]},
            "VAR2",
            PandasDataset,
            [False, False, False],
        ),
        (
            {"target": ["A", "", None], "VAR2": ["", "B", None]},
            "VAR2",
            DaskDataset,
            [True, True, False],
        ),
    ],
)
def test_not_equal_to_null_values(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.not_equal_to({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))
