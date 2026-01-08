from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
import pytest
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


@pytest.mark.parametrize(
    "target, comparator, dataset_type, expected_result",
    [
        (
            "define_variable_name",
            "define_variable_controlled_terms",
            PandasDataset,
            [True, True, True],
        ),
        (
            "define_variable_name",
            "define_variable_controlled_terms",
            DaskDataset,
            [True, True, True],
        ),
        (
            "define_variable_name",
            "define_variable_invalid_terms",
            PandasDataset,
            [True, True, False],
        ),
        (
            "define_variable_name",
            "define_variable_invalid_terms",
            DaskDataset,
            [True, True, False],
        ),
    ],
)
def test_references_correct_codelist(target, comparator, dataset_type, expected_result):
    data = {
        "define_variable_name": ["TEST", "COOLVAR", "ANOTHERVAR"],
        "define_variable_controlled_terms": ["C123", "C456", "C789"],
        "define_variable_invalid_terms": ["C123", "C456", "C786"],
    }

    df = dataset_type.from_dict(data)

    column_codelist_map = {
        "TEST": ["C123", "C456"],
        "COOLVAR": ["C123", "C456"],
        "ANOTHERVAR": ["C789"],
    }
    dft = DataframeType({"value": df, "column_codelist_map": column_codelist_map})

    result = dft.references_correct_codelist(
        {"target": target, "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "target, comparator, dataset_type, expected_result",
    [
        (
            "define_variable_name",
            "define_variable_controlled_terms",
            PandasDataset,
            [False, False, False],
        ),
        (
            "define_variable_name",
            "define_variable_controlled_terms",
            DaskDataset,
            [False, False, False],
        ),
        (
            "define_variable_name",
            "define_variable_invalid_terms",
            PandasDataset,
            [False, False, True],
        ),
        (
            "define_variable_name",
            "define_variable_invalid_terms",
            DaskDataset,
            [False, False, True],
        ),
    ],
)
def test_does_not_reference_correct_codelist(
    target, comparator, dataset_type, expected_result
):
    data = {
        "define_variable_name": ["TEST", "COOLVAR", "ANOTHERVAR"],
        "define_variable_controlled_terms": ["C123", "C456", "C789"],
        "define_variable_invalid_terms": ["C123", "C456", "C786"],
    }

    df = dataset_type.from_dict(data)

    column_codelist_map = {
        "TEST": ["C123", "C456"],
        "COOLVAR": ["C123", "C456"],
        "ANOTHERVAR": ["C789"],
    }
    dft = DataframeType({"value": df, "column_codelist_map": column_codelist_map})

    result = dft.does_not_reference_correct_codelist(
        {"target": target, "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))
