from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
import pytest
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


@pytest.mark.parametrize(
    "target, comparator, dataset_type, expected_result",
    [
        ("ARM", "LAE", PandasDataset, [True, True, True, True]),
        ("ARM", ["ARF"], PandasDataset, [True, True, True, True]),
        ("ARM", ["TAE"], PandasDataset, [False, False, True, True]),
        ("ARM", ["TAE", "NOT_IN_DS"], PandasDataset, [False, False, True, True]),
        ("ARM", ["TAE", ["NOT_IN_DS"]], PandasDataset, [False, False, True, True]),
        ("ARM", ["TAE", ["LAE"]], PandasDataset, [True, True, True, True]),
        ("ARM", [["LAE", "TAE"]], PandasDataset, [True, True, True, True]),
        ("--M", "--F", PandasDataset, [True, True, True, True]),
    ],
)
def test_is_unique_set(target, comparator, dataset_type, expected_result):
    data = {
        "ARM": ["PLACEBO", "PLACEBO", "A", "A"],
        "TAE": [1, 1, 1, 2],
        "LAE": [1, 2, 1, 2],
        "ARF": [1, 2, 3, 4],
    }
    df = dataset_type.from_dict(data)
    result = DataframeType(
        {"value": df, "column_prefix_map": {"--": "AR"}}
    ).is_unique_set({"target": target, "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "target, comparator, dataset_type, expected_result",
    [
        ("ARM", "LAE", PandasDataset, [False, False, False, False]),
        ("ARM", ["ARF"], PandasDataset, [False, False, False, False]),
        ("ARM", ["TAE"], PandasDataset, [True, True, False, False]),
        ("ARM", ["TAE", "NOT_IN_DS"], PandasDataset, [True, True, False, False]),
        ("ARM", ["TAE", ["NOT_IN_DS"]], PandasDataset, [True, True, False, False]),
        ("ARM", ["TAE", ["LAE"]], PandasDataset, [False, False, False, False]),
        ("ARM", [["LAE", "TAE"]], PandasDataset, [False, False, False, False]),
        ("--M", "--F", PandasDataset, [False, False, False, False]),
    ],
)
def test_is_not_unique_set(target, comparator, dataset_type, expected_result):
    data = {
        "ARM": ["PLACEBO", "PLACEBO", "A", "A"],
        "TAE": [1, 1, 1, 2],
        "LAE": [1, 2, 1, 2],
        "ARF": [1, 2, 3, 4],
    }
    df = dataset_type.from_dict(data)
    result = DataframeType(
        {"value": df, "column_prefix_map": {"--": "AR"}}
    ).is_not_unique_set({"target": target, "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "target, comparator, dataset_type, expected_result",
    [
        ("SESEQ", "USUBJID", PandasDataset, True),
        ("UNORDERED", "USUBJID", PandasDataset, False),
        ("SESEQ", "USUBJID", DaskDataset, True),
        ("UNORDERED", "USUBJID", DaskDataset, False),
    ],
)
def test_is_ordered_set(target, comparator, dataset_type, expected_result):
    data = {"USUBJID": [1, 2, 1, 2], "UNORDERED": [3, 1, 2, 2], "SESEQ": [1, 1, 2, 2]}
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df}).is_ordered_set(
        {"target": target, "comparator": comparator}
    )
    assert result == expected_result


@pytest.mark.parametrize(
    "target, comparator, dataset_type, expected_result",
    [
        ("SESEQ", "USUBJID", PandasDataset, False),
        ("UNORDERED", "USUBJID", PandasDataset, True),
        ("SESEQ", "USUBJID", DaskDataset, False),
        ("UNORDERED", "USUBJID", DaskDataset, True),
    ],
)
def test_is_not_ordered_set(target, comparator, dataset_type, expected_result):
    data = {"USUBJID": [1, 2, 1, 2], "UNORDERED": [3, 1, 2, 2], "SESEQ": [1, 1, 2, 2]}
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df}).is_not_ordered_set(
        {"target": target, "comparator": comparator}
    )
    assert result == expected_result


@pytest.mark.parametrize(
    "target, comparator, dataset_type, expected_result",
    [
        ("BGSTRESU", "USUBJID", PandasDataset, [False, False, True, True]),
        ("STRESU", "TESTCD", PandasDataset, [True, True, True, False]),
        ("STRESU", ["TESTCD", "METHOD"], PandasDataset, [False, False, False, False]),
    ],
)
def test_is_inconsistent_across_dataset(
    target, comparator, dataset_type, expected_result
):
    data = {
        "USUBJID": ["SUBJ1", "SUBJ1", "SUBJ2", "SUBJ2"],
        "BGSTRESU": ["kg", "kg", "g", "mg"],
        "TESTCD": ["TEST1", "TEST1", "TEST1", "TEST2"],
        "METHOD": ["M1", "M1", "M2", "M2"],
        "SPEC": ["S1", "S1", "S1", "S1"],
        "STRESU": ["mg", "mg", "g", "kg"],
    }
    df = dataset_type.from_dict(data)
    result = DataframeType(
        {"value": df, "column_prefix_map": {"--": ""}}
    ).is_inconsistent_across_dataset({"target": target, "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "target, comparator, dataset_type, expected_result",
    [
        ("BGSTRESU", "USUBJID", DaskDataset, [False, False, True, True]),
        ("STRESU", "TESTCD", DaskDataset, [True, True, True, False]),
        ("STRESU", ["TESTCD", "METHOD"], DaskDataset, [False, False, False, False]),
    ],
)
def test_is_inconsistent_across_dataset_dask(
    target, comparator, dataset_type, expected_result
):
    data = {
        "USUBJID": ["SUBJ1", "SUBJ1", "SUBJ2", "SUBJ2"],
        "BGSTRESU": ["kg", "kg", "g", "mg"],
        "TESTCD": ["TEST1", "TEST1", "TEST1", "TEST2"],
        "METHOD": ["M1", "M1", "M2", "M2"],
        "SPEC": ["S1", "S1", "S1", "S1"],
        "STRESU": ["mg", "mg", "g", "kg"],
    }
    df = dataset_type.from_dict(data)
    result = DataframeType(
        {"value": df, "column_prefix_map": {"--": ""}}
    ).is_inconsistent_across_dataset({"target": target, "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "target, comparator, dataset_type, expected_result",
    [
        ("BGSTRESU", "USUBJID", PandasDataset, [False, False, True, True]),
    ],
)
def test_is_inconsistent_across_dataset_with_nulls(
    target, comparator, dataset_type, expected_result
):
    data = {
        "USUBJID": ["SUBJ1", "SUBJ1", "SUBJ2", "SUBJ2"],
        "BGSTRESU": ["kg", "kg", None, "g"],
    }
    df = dataset_type.from_dict(data)
    result = DataframeType(
        {"value": df, "column_prefix_map": {"--": ""}}
    ).is_inconsistent_across_dataset({"target": target, "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


def test_is_inconsistent_across_dataset_empty_dataset():
    data = {"USUBJID": [], "BGSTRESU": []}
    df = PandasDataset.from_dict(data)
    result = DataframeType(
        {"value": df, "column_prefix_map": {"--": ""}}
    ).is_inconsistent_across_dataset({"target": "BGSTRESU", "comparator": "USUBJID"})
    assert len(result) == 0
