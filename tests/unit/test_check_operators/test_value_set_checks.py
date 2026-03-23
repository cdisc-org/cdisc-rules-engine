from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
import pytest
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
import pandas as pd


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
    "target, comparator, regex, dataset_type, expected_result",
    [
        (
            "ARM",
            "DTC",
            r"^\d{4}-\d{2}-\d{2}",
            PandasDataset,
            [False, False, False, False],
        ),
        ("ARM", "TAE", None, PandasDataset, [False, False, True, True]),
    ],
)
def test_is_unique_set_with_regex(
    target, comparator, regex, dataset_type, expected_result
):
    data = {
        "ARM": ["PLACEBO", "PLACEBO", "ACTIVE", "ACTIVE"],
        "TAE": [1, 1, 1, 2],
        "DTC": [
            "2024-01-15T10:30:00",
            "2024-01-15T14:45:00",
            "2024-01-16T10:30:00",
            "2024-01-16T14:45:00",
        ],
    }
    df = dataset_type.from_dict(data)
    params = {"target": target, "comparator": comparator}
    if regex is not None:
        params["regex"] = regex
    result = DataframeType(
        {"value": df, "column_prefix_map": {"--": "AR"}}
    ).is_unique_set(params)
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "target, comparator, dataset_type, expected_result",
    [
        (
            "SESEQ",
            "USUBJID",
            PandasDataset,
            pd.Series([True, True, True, True]),
        ),
        (
            "UNORDERED",
            "USUBJID",
            PandasDataset,
            pd.Series([False, True, False, True]),
        ),
        (
            "SESEQ",
            "USUBJID",
            DaskDataset,
            pd.Series([True, True, True, True]),
        ),
        (
            "UNORDERED",
            "USUBJID",
            DaskDataset,
            pd.Series([False, True, False, True]),
        ),
        (
            "SESEQ",
            ["USUBJID"],
            PandasDataset,
            pd.Series([True, True, True, True]),
        ),
        (
            "UNORDERED",
            ["USUBJID"],
            PandasDataset,
            pd.Series([False, True, False, True]),
        ),
    ],
)
def test_is_ordered_set(target, comparator, dataset_type, expected_result):
    data = {"USUBJID": [1, 2, 1, 2], "UNORDERED": [3, 1, 2, 2], "SESEQ": [1, 1, 2, 2]}
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df}).is_ordered_set(
        {"target": target, "comparator": comparator}
    )
    pd.testing.assert_series_equal(result, expected_result, check_names=False)


@pytest.mark.parametrize(
    "target, comparator, dataset_type, expected_result",
    [
        (
            "SESEQ",
            "USUBJID",
            PandasDataset,
            pd.Series([False, False, False, False]),
        ),
        (
            "UNORDERED",
            "USUBJID",
            PandasDataset,
            pd.Series([True, False, True, False]),
        ),
        (
            "SESEQ",
            "USUBJID",
            DaskDataset,
            pd.Series([False, False, False, False]),
        ),
        (
            "UNORDERED",
            "USUBJID",
            DaskDataset,
            pd.Series([True, False, True, False]),
        ),
    ],
)
def test_is_not_ordered_set(target, comparator, dataset_type, expected_result):
    data = {"USUBJID": [1, 2, 1, 2], "UNORDERED": [3, 1, 2, 2], "SESEQ": [1, 1, 2, 2]}
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df}).is_not_ordered_set(
        {"target": target, "comparator": comparator}
    )
    pd.testing.assert_series_equal(result, expected_result, check_names=False)


def test_is_ordered_set_multiple_comparators():
    data = {
        "ARMCD": [
            "PLACEBO",
            "PLACEBO",
            "ZAN_LOW",
            "ZAN_LOW",
            "ZAN_HIGH",
            "ZAN_HIGH",
            "ZAN_HIGH",
            "ZAN_HIGH",
        ],
        "ARM": [
            "Placebo",
            "Placebo",
            "Zanomaline Low Dose",
            "Zanomaline Low Dose",
            "Zanomaline High Dose",
            "Zanomaline High Dose",
            "Zanomaline High Dose",
            "Zanomaline High Dose",
        ],
        "TAETORD": [1, 2, 1, 2, 1, 2, 3, 2],
    }
    df = PandasDataset.from_dict(data)
    result = DataframeType({"value": df}).is_ordered_set(
        {"target": "TAETORD", "comparator": ["ARMCD", "ARM"]}
    )
    pd.testing.assert_series_equal(
        result,
        pd.Series([True, True, True, True, True, True, False, False]),
        check_names=False,
    )


@pytest.mark.parametrize(
    "target, comparator, dataset_type, expected_result",
    [
        ("BGSTRESU", "USUBJID", PandasDataset, [False, False, True, True]),
        ("STRESU", "TESTCD", PandasDataset, [False, False, True, False]),
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
    "values,regex,expected",
    [
        # regex disabled
        (["A", "B"], None, [True, True]),
        (["TEST_v1", "TEST_v2"], "", [True, True]),
        (["TEST", "TEST"], "", [False, False]),
        # regex collapsing values
        (["TEST_v1", "TEST_v2"], r"^(TEST)", [False, False]),
        (["ABC123", "XYZ123"], r"(\d+)", [False, False]),
        (["HEIGHT_cm", "HEIGHT_mm"], r"^(HEIGHT)", [False, False]),
        # datetime normalization
        (
            ["2014-09-30T11:09", "2014-09-30T11:07"],
            r"^(\d{4}-\d{2}-\d{2})",
            [False, False],
        ),
        (["TEST_A", "TEST_B"], r"^(TEST_[A-Z])", [True, True]),
        (["SUBJ-001", "SUBJ-002"], r"SUBJ-(\d+)", [True, True]),
        (
            ["2014-09-30T11:09", "2014-09-29T11:07"],
            r"^(\d{4}-\d{2}-\d{2})",
            [True, True],
        ),
        # regex no capture group
        (["ABC", "DEF"], r"^XYZ", [True, True]),
        (["TEST_v1", "CONTROL"], r"^(TEST)", [True, True]),
        (["A", "B"], r"(.*)", [True, True]),
        (["A", None], r"(A)", [True, True]),
        ([None, None], r"(.*)", [False, False]),
        ([1, 1], r"(\d+)", [False, False]),
        # multiple capture groups
        ([1, 1], r"(\d+)(\d+)", [False, False]),
        # multiple regex
        (
            [1, 1],
            [
                r"(\d+)(\d+)",
                r"(\d+)(\d+)",
            ],
            [False, False],
        ),
    ],
)
def test_is_inconsistent_across_dataset_regex(values, regex, expected):
    df = pd.DataFrame(
        {
            "VISIT": ["WEEK1"] * len(values),
            "EPOCH": ["TREATMENT"] * len(values),
            "VALUE": values,
        }
    )

    other_value = {
        "target": "VALUE",
        "comparator": ["VISIT", "EPOCH"],
        "regex": regex,
    }

    obj = DataframeType(
        {
            "value": df,
        }
    )
    result = obj.is_inconsistent_across_dataset(other_value)

    assert result.tolist() == expected


@pytest.mark.parametrize(
    "values,regex,expected",
    [
        (["TEST_v1", "TEST_v2"], "AABB???", [True, True]),
        (["TEST_v1", "TEST_v2"], "AA(C(B)A", [True, True]),
        (["TEST_v1", "TEST_v2"], "AA(C)B)A", [True, True]),
        (["TEST_v1", "TEST_v2"], "\\", [True, True]),
        (["TEST_v1", "TEST_v2"], "**", [True, True]),
        (["TEST", "TEST"], "AABB???", [False, False]),
        (["TEST", "TEST"], "AA(C(B)A", [False, False]),
        (["TEST", "TEST"], "AA(C)B)A", [False, False]),
        (["TEST", "TEST"], "\\", [False, False]),
        (["TEST", "TEST"], "**", [False, False]),
    ],
)
def test_is_inconsistent_across_dataset_regex_ignores_bad_regex(
    values, regex, expected
):
    df = pd.DataFrame(
        {
            "VISIT": ["WEEK1"] * len(values),
            "EPOCH": ["TREATMENT"] * len(values),
            "VALUE": values,
        }
    )

    other_value = {
        "target": "VALUE",
        "comparator": ["VISIT", "EPOCH"],
        "regex": regex,
    }

    obj = DataframeType(
        {
            "value": df,
        }
    )
    with pytest.raises(ValueError):
        obj.is_inconsistent_across_dataset(other_value)


@pytest.mark.parametrize(
    "target, comparator, dataset_type, expected_result",
    [
        ("BGSTRESU", "USUBJID", DaskDataset, [False, False, True, True]),
        ("STRESU", "TESTCD", DaskDataset, [False, False, True, False]),
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


@pytest.mark.parametrize(
    "target, comparator, dataset_type, expected_result",
    [
        ("VALUE", "GROUP", PandasDataset, [False, False, False, True]),
    ],
)
def test_is_inconsistent_clear_majority(
    target, comparator, dataset_type, expected_result
):
    data = {
        "GROUP": ["GROUP1", "GROUP1", "GROUP1", "GROUP1"],
        "VALUE": ["A", "A", "A", "B"],
    }
    df = dataset_type.from_dict(data)
    result = DataframeType(
        {"value": df, "column_prefix_map": {"--": ""}}
    ).is_inconsistent_across_dataset({"target": target, "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "target, comparator, dataset_type, expected_result",
    [
        ("VALUE", "GROUP", PandasDataset, [True, True, False]),
    ],
)
def test_is_inconsistent_perfect_tie(target, comparator, dataset_type, expected_result):
    data = {
        "GROUP": ["GROUP1", "GROUP1", "GROUP2"],
        "VALUE": ["A", "B", "X"],
    }
    df = dataset_type.from_dict(data)
    result = DataframeType(
        {"value": df, "column_prefix_map": {"--": ""}}
    ).is_inconsistent_across_dataset({"target": target, "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "target, comparator, dataset_type, expected_result",
    [
        ("VALUE", "GROUP", PandasDataset, [True, True, True]),
    ],
)
def test_is_inconsistent_three_way_tie(
    target, comparator, dataset_type, expected_result
):
    data = {
        "GROUP": ["GROUP1", "GROUP1", "GROUP1"],
        "VALUE": ["A", "B", "C"],
    }
    df = dataset_type.from_dict(data)
    result = DataframeType(
        {"value": df, "column_prefix_map": {"--": ""}}
    ).is_inconsistent_across_dataset({"target": target, "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))
