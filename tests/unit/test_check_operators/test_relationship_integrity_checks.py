from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
import pytest
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
import pandas as pd


@pytest.mark.parametrize(
    "data,comparator,within,dataset_type,expected_result",
    [
        (
            {
                "USUBJID": [
                    1,
                    1,
                    1,
                    2,
                    2,
                    2,
                ],
                "SEQ": [1, 2, 3, 4, 5, 6],
                "target": [
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP3",
                    "AEHOSP2",
                    "AEHOSP2",
                ],
            },
            1,
            "USUBJID",
            DaskDataset,
            [True, True, True, True, True, False],
        ),
        (
            {
                "USUBJID": [
                    1,
                    1,
                    1,
                    2,
                    2,
                    2,
                ],
                "SEQ": [1, 2, 3, 4, 5, 6],
                "target": [
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP3",
                    "AEHOSP2",
                    "AEHOSP2",
                ],
            },
            1,
            "USUBJID",
            PandasDataset,
            [True, True, True, False, True, True],
        ),
    ],
)
def test_present_on_multiple_rows_within(
    data, comparator, within, dataset_type, expected_result
):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.present_on_multiple_rows_within(
        {"target": "target", "comparator": comparator, "within": within}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data, dataset_type, expected_result",
    [
        (
            {
                "USUBJID": [
                    1,
                    1,
                    1,
                    2,
                    2,
                    2,
                ],
                "SEQ": [1, 2, 3, 4, 5, 6],
                "target": [
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP3",
                    "AEHOSP2",
                    "AEHOSP2",
                ],
            },
            PandasDataset,
            [True, True, True, True, True, True],
        ),
    ],
)
def test_has_different_values(data, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.has_different_values({"target": "target"})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data, dataset_type, expected_result",
    [
        (
            {
                "USUBJID": [
                    1,
                    1,
                    1,
                    2,
                    2,
                    2,
                ],
                "SEQ": [1, 2, 3, 4, 5, 6],
                "target": [
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP3",
                    "AEHOSP2",
                    "AEHOSP2",
                ],
            },
            PandasDataset,
            [False, False, False, False, False, False],
        ),
        (
            {
                "USUBJID": [
                    1,
                    1,
                    1,
                    2,
                    2,
                    2,
                ],
                "SEQ": [1, 2, 3, 4, 5, 6],
                "target": [
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                ],
            },
            DaskDataset,
            [True, True, True, True, True, True],
        ),
    ],
)
def test_has_same_values(data, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.has_same_values({"target": "target"})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {
                "STUDYID": [
                    "TEST",
                    "TEST-1",
                    "TEST-2",
                    "TEST-3",
                ],
                "VISITNUM": [1, 2, 1, 3],
                "target": [
                    "Consulting",
                    "Surgery",
                    "Consulting",
                    "Treatment",
                ],
            },
            "VISITNUM",
            PandasDataset,
            [False, False, False, False],
        ),
        (
            {
                "STUDYID": [
                    "TEST",
                    "TEST-1",
                    "TEST-2",
                    "TEST-3",
                ],
                "VISITNUM": [1, 2, 1, 3],
                "target": [
                    "Consulting",
                    "Surgery",
                    "Surgery",
                    "Treatment",
                ],
            },
            "VISITNUM",
            DaskDataset,
            [True, True, True, False],
        ),
    ],
)
def test_is_not_unique_relationship(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.is_not_unique_relationship(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,context,dataset_type,expected_result",
    [
        (
            {
                "RDOMAIN": ["LB", "LB", "AE"],
                "target": ["TEST", "DATA", "AETERM"],
                "IDVARVAL1": [4, 1, 31],
                "IDVARVAL2": [5, 1, 35],
            },
            "IDVARVAL1",
            "RDOMAIN",
            DaskDataset,
            [True, True, True],
        ),
        (
            {
                "RDOMAIN": ["LB", "LB", "AE"],
                "target": ["TEST", "DATA", "AETERM"],
                "IDVARVAL1": [4, 1, 31],
                "IDVARVAL2": [5, 1, 35],
            },
            "IDVARVAL2",
            "RDOMAIN",
            PandasDataset,
            [True, True, False],
        ),
    ],
)
def test_valid_relationship(data, comparator, context, dataset_type, expected_result):
    reference_data = {
        "LB": {
            "TEST": pd.Series([4, 5, 6]).values,
            "DATA": pd.Series([1, 2, 3]).values,
        },
        "AE": {"AETERM": pd.Series([31, 323, 33]).values},
    }
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df, "relationship_data": reference_data})
    result = dataframe_type.is_valid_relationship(
        {"target": "target", "comparator": comparator, "context": context}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,context,dataset_type,expected_result",
    [
        (
            {
                "RDOMAIN": ["LB", "LB", "AE"],
                "target": ["TEST", "DATA", "AETERM"],
                "IDVARVAL1": [4, 1, 31],
                "IDVARVAL2": [5, 1, 35],
            },
            "IDVARVAL1",
            "RDOMAIN",
            PandasDataset,
            [False, False, False],
        ),
        (
            {
                "RDOMAIN": ["LB", "LB", "AE"],
                "target": ["TEST", "DATA", "AETERM"],
                "IDVARVAL1": [4, 1, 31],
                "IDVARVAL2": [5, 1, 35],
            },
            "IDVARVAL2",
            "RDOMAIN",
            DaskDataset,
            [False, False, True],
        ),
    ],
)
def test_is_not_valid_relationship(
    data, comparator, context, dataset_type, expected_result
):
    reference_data = {
        "LB": {
            "TEST": pd.Series([4, 5, 6]).values,
            "DATA": pd.Series([1, 2, 3]).values,
        },
        "AE": {"AETERM": pd.Series([31, 323, 33]).values},
    }
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df, "relationship_data": reference_data})
    result = dataframe_type.is_not_valid_relationship(
        {"target": "target", "comparator": comparator, "context": context}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,context,dataset_type,expected_result",
    [
        (
            {
                "RDOMAIN": ["LB", "LB", "AE"],
                "target": ["TEST", "DATA", "AETERM"],
            },
            "RDOMAIN",
            PandasDataset,
            [True, True, True],
        ),
        (
            {"RDOMAIN": ["LB", "LB", "AE"], "target": ["TEST", "AETERM", "AETERM"]},
            "RDOMAIN",
            DaskDataset,
            [True, False, True],
        ),
    ],
)
def test_is_valid_reference(data, context, dataset_type, expected_result):
    reference_data = {
        "LB": {"TEST": [], "DATA": [1, 2, 3]},
        "AE": {"AETERM": [1, 2, 3]},
    }
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df, "relationship_data": reference_data})
    result = dataframe_type.is_valid_reference({"target": "target", "context": context})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,context,dataset_type,expected_result",
    [
        (
            {
                "RDOMAIN": ["LB", "LB", "AE"],
                "target": ["TEST", "DATA", "AETERM"],
            },
            "RDOMAIN",
            DaskDataset,
            [False, False, False],
        ),
        (
            {
                "RDOMAIN": ["LB", "LB", "AE"],
                "target": ["TEST", "AETERM", "AETERM"],
            },
            "RDOMAIN",
            PandasDataset,
            [False, True, False],
        ),
    ],
)
def test_is_not_valid_reference(data, context, dataset_type, expected_result):
    reference_data = {
        "LB": {"TEST": [], "DATA": [1, 2, 3]},
        "AE": {"AETERM": [1, 2, 3]},
    }
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df, "relationship_data": reference_data})
    result = dataframe_type.is_not_valid_reference(
        {"target": "target", "context": context}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize("dataset_class", [PandasDataset, DaskDataset])
def test_empty_within_except_last_row(dataset_class):
    df = dataset_class.from_dict(
        {
            "USUBJID": [1, 1, 1, 2, 2, 2],
            "valid": [
                "2020-10-10",
                "2020-10-10",
                "2020-10-10",
                "2021",
                "2021",
                "2021",
            ],
            "invalid": [
                "2020-10-10",
                None,
                None,
                "2020",
                "2020",
                None,
            ],
        }
    )
    valid_df = dataset_class.from_dict(
        {
            "USUBJID": [
                789,
                789,
                789,
                789,
                790,
                790,
                790,
                790,
            ],
            "SESEQ": [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
            ],
            "SEENDTC": [
                "2006-06-03T10:32",
                "2006-06-10T09:47",
                "2006-06-17",
                "",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
                "2006-06-17",
            ],
            "SESTDTC": [
                "2006-06-01",
                "2006-06-03T10:32",
                "2006-06-10T09:47",
                "2006-06-17",
                "2006-06-01",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
            ],
        }
    )
    invalid_df = dataset_class.from_dict(
        {
            "USUBJID": [
                789,
                789,
                789,
                789,
                790,
                790,
                790,
                790,
            ],
            "SESEQ": [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
            ],
            "SEENDTC": [
                "",
                "2006-06-10T09:47",
                "2006-06-17",
                "",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
                "2006-06-17",
            ],
            "SESTDTC": [
                "2006-06-01",
                "2006-06-03T10:32",
                "2006-06-10T09:47",
                "2006-06-17",
                "2006-06-01",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
            ],
        }
    )
    assert (
        not DataframeType({"value": df})
        .empty_within_except_last_row({"target": "valid", "comparator": "USUBJID"})
        .equals(pd.Series({0: True, 1: True, 3: True, 4: True}))
    )
    assert (
        DataframeType({"value": df})
        .empty_within_except_last_row({"target": "invalid", "comparator": "USUBJID"})
        .equals(pd.Series({0: False, 1: True, 3: False, 4: False}))
    )
    assert (
        DataframeType({"value": valid_df})
        .empty_within_except_last_row(
            {"target": "SEENDTC", "ordering": "SESTDTC", "comparator": "USUBJID"}
        )
        .equals(pd.Series({0: False, 1: False, 2: False, 4: False, 5: False, 6: False}))
    )
    assert (
        DataframeType({"value": invalid_df})
        .empty_within_except_last_row(
            {"target": "SEENDTC", "ordering": "SESTDTC", "comparator": "USUBJID"}
        )
        .equals(pd.Series({0: True, 1: False, 2: False, 4: False, 5: False, 6: False}))
    )


@pytest.mark.parametrize("dataset_class", [PandasDataset, DaskDataset])
def test_non_empty_within_except_last_row(dataset_class):
    df = dataset_class.from_dict(
        {
            "USUBJID": [1, 1, 1, 2, 2, 2],
            "valid": [
                "2020-10-10",
                "2020-10-10",
                "2020-10-10",
                "2021",
                "2021",
                "2021",
            ],
            "invalid": [
                "2020-10-10",
                None,
                None,
                "2020",
                "2020",
                None,
            ],
        }
    )
    valid_df = dataset_class.from_dict(
        {
            "USUBJID": [
                789,
                789,
                789,
                789,
                790,
                790,
                790,
                790,
            ],
            "SESEQ": [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
            ],
            "SEENDTC": [
                "2006-06-03T10:32",
                "2006-06-10T09:47",
                "2006-06-17",
                "",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
                "2006-06-17",
            ],
            "SESTDTC": [
                "2006-06-01",
                "2006-06-03T10:32",
                "2006-06-10T09:47",
                "2006-06-17",
                "2006-06-01",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
            ],
        }
    )
    invalid_df = dataset_class.from_dict(
        {
            "USUBJID": [
                789,
                789,
                789,
                789,
                790,
                790,
                790,
                790,
            ],
            "SESEQ": [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
            ],
            "SEENDTC": [
                "",
                "2006-06-10T09:47",
                "2006-06-17",
                "",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
                "2006-06-17",
            ],
            "SESTDTC": [
                "2006-06-01",
                "2006-06-03T10:32",
                "2006-06-10T09:47",
                "2006-06-17",
                "2006-06-01",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
            ],
        }
    )
    assert (
        DataframeType({"value": df})
        .non_empty_within_except_last_row({"target": "valid", "comparator": "USUBJID"})
        .equals(pd.Series({0: True, 1: True, 3: True, 4: True}))
    )
    assert (
        not DataframeType({"value": df})
        .non_empty_within_except_last_row(
            {"target": "invalid", "comparator": "USUBJID"}
        )
        .equals(pd.Series({0: False, 1: True, 3: False, 4: False}))
    )
    assert (
        DataframeType({"value": valid_df})
        .non_empty_within_except_last_row(
            {"target": "SEENDTC", "ordering": "SESTDTC", "comparator": "USUBJID"}
        )
        .equals(pd.Series({0: True, 1: True, 2: True, 4: True, 5: True, 6: True}))
    )
    assert (
        DataframeType({"value": invalid_df})
        .non_empty_within_except_last_row(
            {"target": "SEENDTC", "ordering": "SESTDTC", "comparator": "USUBJID"}
        )
        .equals(pd.Series({0: False, 1: True, 2: True, 4: True, 5: True, 6: True}))
    )


@pytest.mark.parametrize("dataset_class", [PandasDataset, DaskDataset])
def test_has_next_corresponding_record(dataset_class):
    """
    Test for has_next_corresponding_record operator.
    """
    valid_df = dataset_class.from_dict(
        {
            "USUBJID": [
                789,
                789,
                789,
                789,
                790,
                790,
                790,
                790,
            ],
            "SESEQ": [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
            ],
            "SEENDTC": [
                "2006-06-03T10:32",
                "2006-06-10T09:47",
                "2006-06-17",
                "2006-06-17",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
                "2006-06-17",
            ],
            "SESTDTC": [
                "2006-06-01",
                "2006-06-03T10:32",
                "2006-06-10T09:47",
                "2006-06-17",
                "2006-06-01",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
            ],
        }
    )
    other_value: dict = {
        "target": "SEENDTC",
        "comparator": "SESTDTC",
        "within": "USUBJID",
        "ordering": "SESEQ",
    }
    result = DataframeType({"value": valid_df}).has_next_corresponding_record(
        other_value
    )
    assert result.equals(pd.Series([True, True, True, True, True, True, True, True]))

    invalid_df = dataset_class.from_dict(
        {
            "USUBJID": [
                789,
                789,
                789,
                789,
                790,
                790,
                790,
                790,
            ],
            "SESEQ": [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
            ],
            "SEENDTC": [
                "2006-06-03T10:32",
                "2006-06-10T09:47",
                "2006-06-17",
                "2006-06-17",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
                "2006-06-17",
            ],
            "SESTDTC": [
                "2006-06-01",
                "2010-08-03",
                "2008-08",
                "2006-06-17T10:20",
                "2006-06-01",
                "2006-06-03T10:14",
                "2006-06-10T10:32",
                "2006-06-17",
            ],
        }
    )
    other_value: dict = {
        "target": "SEENDTC",
        "comparator": "SESTDTC",
        "within": "USUBJID",
        "ordering": "SESEQ",
    }
    result = DataframeType({"value": invalid_df}).has_next_corresponding_record(
        other_value
    )
    assert result.equals(pd.Series([False, False, False, True, True, True, True, True]))


@pytest.mark.parametrize("dataset_class", [PandasDataset, DaskDataset])
def test_target_is_sorted_by(dataset_class):
    """
    Unit test for target_is_sorted_by  operator.
    The test verifies if --SEQ is  sorted based on set of  user-defined columns
    """
    valid_asc_df = dataset_class.from_dict(
        {
            "USUBJID": ["CDISC001", "CDISC002", "CDISC002", "CDISC001", "CDISC001"],
            "SESEQ": [1, 2, 1, 3, 2],
            "SESTDTC": [
                "2006-06-02",
                "2006-06-04",
                "2006-06-01",
                "2006-06-05",
                "2006-06-03",
            ],
        }
    )

    other_value: dict = {
        "target": "--SEQ",
        "within": "USUBJID",
        "comparator": [
            {"name": "--STDTC", "sort_order": "ASC", "null_position": "last"}
        ],
    }
    result = DataframeType(
        {"value": valid_asc_df, "column_prefix_map": {"--": "SE"}}
    ).target_is_sorted_by(other_value)
    assert result.equals(
        pd.Series(
            [
                True,
                True,
                True,
                True,
                True,
            ]
        )
    )

    valid_desc_df = dataset_class.from_dict(
        {
            "USUBJID": ["CDISC001", "CDISC002", "CDISC002", "CDISC001", "CDISC001"],
            "SESEQ": [3, 2, 1, 2, 1],
            "SESTDTC": [
                "2006-06-05",
                "2006-06-04",
                "2006-06-01",
                "2006-06-03",
                "2006-06-02",
            ],
        }
    )

    other_value: dict = {
        "target": "--SEQ",
        "within": "USUBJID",
        "comparator": [
            {"name": "--STDTC", "sort_order": "DESC", "null_position": "last"}
        ],
    }
    result = DataframeType(
        {"value": valid_desc_df, "column_prefix_map": {"--": "SE"}}
    ).target_is_sorted_by(other_value)
    assert result.equals(
        pd.Series(
            [
                False,
                False,
                False,
                True,
                False,
            ]
        )
    )

    valid_asc_df = dataset_class.from_dict(
        {
            "USUBJID": [123, 456, 456, 123, 123],
            "SESEQ": [1, 2, 1, 3, 2],
            "SESTDTC": [
                "2006-06-02",
                "2006-06-04",
                "2006-06-01",
                "2006-06-05",
                "2006-06-03",
            ],
        }
    )
    other_value: dict = {
        "target": "--SEQ",
        "within": "USUBJID",
        "comparator": [
            {"name": "--STDTC", "sort_order": "ASC", "null_position": "last"}
        ],
    }
    result = DataframeType(
        {"value": valid_asc_df, "column_prefix_map": {"--": "SE"}}
    ).target_is_sorted_by(other_value)
    assert result.equals(
        pd.Series(
            [
                True,
                True,
                True,
                True,
                True,
            ]
        )
    )

    valid_desc_df = dataset_class.from_dict(
        {
            "USUBJID": [123, 456, 456, 123, 123],
            "SESEQ": [1, 2, 1, 3, 2],
            "SESTDTC": [
                "2006-06-02",
                "2006-06-04",
                "2006-06-01",
                "2006-06-05",
                "2006-06-03",
            ],
        }
    )
    other_value: dict = {
        "target": "--SEQ",
        "within": "USUBJID",
        "comparator": [
            {"name": "--STDTC", "sort_order": "DESC", "null_position": "last"}
        ],
    }
    result = DataframeType(
        {"value": valid_desc_df, "column_prefix_map": {"--": "SE"}}
    ).target_is_sorted_by(other_value)
    assert result.equals(
        pd.Series(
            [
                False,
                False,
                False,
                False,
                True,
            ]
        )
    )

    invalid_df = dataset_class.from_dict(
        {
            "USUBJID": ["CDISC001", "CDISC002", "CDISC002", "CDISC001", "CDISC001"],
            "SESEQ": [1, 2, 3, 3, 2],
            "SESTDTC": [
                "2006-06-02",
                "2006-06-04",
                "2006-06-01",
                "2006-06-05",
                "2006-06-03",
            ],
        }
    )

    other_value: dict = {
        "target": "--SEQ",
        "within": "USUBJID",
        "comparator": [
            {"name": "--STDTC", "sort_order": "ASC", "null_position": "last"}
        ],
    }
    result = DataframeType(
        {"value": invalid_df, "column_prefix_map": {"--": "SE"}}
    ).target_is_sorted_by(other_value)
    assert result.equals(
        pd.Series(
            [
                True,
                False,
                False,
                True,
                True,
            ]
        )
    )

    valid_mul_df = dataset_class.from_dict(
        {
            "USUBJID": ["CDISC001", "CDISC002", "CDISC002", "CDISC001", "CDISC001"],
            "SESEQ": [1, 2, 1, 3, 2],
            "SESTDTC": [
                "2006-06-02",
                "2006-06-04",
                "2006-06-01",
                "2006-06-05",
                "2006-06-03",
            ],
            "STUDYID": [
                "CDISCPILOT1",
                "CDISCPILOT1",
                "CDISCPILOT1",
                "CDISCPILOT1",
                "CDISCPILOT1",
            ],
            "SEENDTC": [
                "2006-06-02",
                "2006-06-04",
                "2006-06-01",
                "2006-06-05",
                "2006-06-03",
            ],
        }
    )

    other_value: dict = {
        "target": "--SEQ",
        "within": "USUBJID",
        "comparator": [
            {"name": "--STDTC", "sort_order": "ASC", "null_position": "last"},
            {"name": "--ENDTC", "sort_order": "ASC", "null_position": "last"},
        ],
    }
    result = DataframeType(
        {"value": valid_mul_df, "column_prefix_map": {"--": "SE"}}
    ).target_is_sorted_by(other_value)
    assert result.equals(
        pd.Series(
            [
                True,
                True,
                True,
                True,
                True,
            ]
        )
    )

    valid_mul_df = dataset_class.from_dict(
        {
            "USUBJID": ["CDISC001", "CDISC002", "CDISC002", "CDISC001", "CDISC001"],
            "SESEQ": [7, 1, 2, 8, 6],
            "SESTDTC": [
                "2006-06-03",
                "2006-06-04",
                "2006-06-01",
                "2006-06-05",
                "2006-06-01",
            ],
            "STUDYID": [
                "CDISCPILOT1",
                "CDISCPILOT1",
                "CDISCPILOT1",
                "CDISCPILOT1",
                "CDISCPILOT1",
            ],
            "SEENDTC": [
                "2006-06-03",
                "2006-06-04",
                "2006-06-01",
                "2006-06-05",
                "2006-06-01",
            ],
        }
    )

    other_value: dict = {
        "target": "--SEQ",
        "within": "USUBJID",
        "comparator": [
            {"name": "--STDTC", "sort_order": "DESC", "null_position": "last"},
            {"name": "--ENDTC", "sort_order": "DESC", "null_position": "last"},
        ],
    }
    result = DataframeType(
        {"value": valid_mul_df, "column_prefix_map": {"--": "SE"}}
    ).target_is_sorted_by(other_value)
    assert result.equals(
        pd.Series(
            [
                True,
                True,
                True,
                False,
                False,
            ]
        )
    )

    valid_mul_df = dataset_class.from_dict(
        {
            "USUBJID": ["CDISC001", "CDISC001", "CDISC001", "CDISC001", "CDISC001"],
            "SESEQ": [1, 2, 5, 8, 12],
            "SESTDTC": [
                "2006-06-01",
                "2006-06-02",
                "2006-06-03",
                "2006-06-04",
                "2006-06-05",
            ],
            "STUDYID": [
                "CDISCPILOT1",
                "CDISCPILOT1",
                "CDISCPILOT1",
                "CDISCPILOT1",
                "CDISCPILOT1",
            ],
            "SEENDTC": [
                "2006-06-04",
                "2006-06-05",
                "2006-06-06",
                "2006-06-07",
                "2006-06-08",
            ],
        }
    )

    other_value: dict = {
        "target": "--SEQ",
        "within": "USUBJID",
        "comparator": [
            {"name": "--STDTC", "sort_order": "ASC", "null_position": "last"},
            {"name": "--ENDTC", "sort_order": "DESC", "null_position": "last"},
        ],
    }
    result = DataframeType(
        {"value": valid_mul_df, "column_prefix_map": {"--": "SE"}}
    ).target_is_sorted_by(other_value)
    assert result.equals(
        pd.Series(
            [
                False,
                False,
                True,
                False,
                False,
            ]
        )
    )

    invalid_mul_df = dataset_class.from_dict(
        {
            "USUBJID": ["CDISC001", "CDISC002", "CDISC002", "CDISC001", "CDISC001"],
            "SESEQ": [1, 2, 1, 1, 2],
            "SESTDTC": [
                "2006-06-02",
                "2006-06-04",
                "2006-06-01",
                "2006-06-05",
                "2006-06-03",
            ],
            "STUDYID": [
                "CDISCPILOT1",
                "CDISCPILOT1",
                "CDISCPILOT1",
                "CDISCPILOT1",
                "CDISCPILOT1",
            ],
            "SEENDTC": [
                "2006-06-02",
                "2006-06-04",
                "2006-06-01",
                "2006-06-05",
                "2006-06-03",
            ],
        }
    )

    other_value: dict = {
        "target": "--SEQ",
        "within": "USUBJID",
        "comparator": [
            {"name": "--STDTC", "sort_order": "ASC", "null_position": "last"},
            {"name": "--ENDTC", "sort_order": "ASC", "null_position": "last"},
        ],
    }
    result = DataframeType(
        {"value": invalid_mul_df, "column_prefix_map": {"--": "SE"}}
    ).target_is_sorted_by(other_value)
    assert result.equals(
        pd.Series(
            [
                True,
                True,
                True,
                False,
                False,
            ]
        )
    )

    valid_na_df = dataset_class.from_dict(
        {
            "USUBJID": [123, 456, 456, 123, 123],
            "SESEQ": [1, 2, 1, None, None],
            "SESTDTC": ["2006-06-02", None, "2006-06-01", None, "2006-06-03"],
        }
    )

    other_value: dict = {
        "target": "--SEQ",
        "within": "USUBJID",
        "comparator": [
            {"name": "--STDTC", "sort_order": "ASC", "null_position": "last"}
        ],
    }
    result = DataframeType(
        {"value": valid_na_df, "column_prefix_map": {"--": "SE"}}
    ).target_is_sorted_by(other_value)
    assert result.equals(
        pd.Series(
            [
                True,
                False,
                True,
                False,
                True,
            ]
        )
    )

    invalid_na_df = dataset_class.from_dict(
        {
            "USUBJID": [123, 456, 456, 123, 123],
            "SESEQ": [1, 2, 3, None, None],
            "SESTDTC": ["2006-06-02", None, "2006-06-01", None, "2006-06-03"],
        }
    )

    other_value: dict = {
        "target": "--SEQ",
        "within": "USUBJID",
        "comparator": [
            {"name": "--STDTC", "sort_order": "ASC", "null_position": "last"}
        ],
    }
    result = DataframeType(
        {"value": invalid_na_df, "column_prefix_map": {"--": "SE"}}
    ).target_is_sorted_by(other_value)
    assert result.equals(
        pd.Series(
            [
                True,
                False,
                False,
                False,
                True,
            ]
        )
    )


@pytest.mark.parametrize("dataset_class", [PandasDataset])
def test_target_is_sorted_by_datetime(dataset_class):
    """
    Test target_is_sorted_by with datetime comparisons
    """
    datetime_df = dataset_class.from_dict(
        {
            "USUBJID": ["CDISC001", "CDISC001", "CDISC002", "CDISC002", "CDISC003"],
            "SESEQ": [1, 2, 1, 2, 1],
            "SESTDTC": [
                "2006-06-02 10:00",
                "2006-06-02 14:30:00",
                "2006-06-03 09:15",
                "2006-06-03 11:45:00",
                "2006-06-04 08:00:00",
            ],
        }
    )

    other_value: dict = {
        "target": "--SEQ",
        "within": "USUBJID",
        "comparator": [
            {"name": "--STDTC", "sort_order": "ASC", "null_position": "last"}
        ],
    }
    result = DataframeType(
        {"value": datetime_df, "column_prefix_map": {"--": "SE"}}
    ).target_is_sorted_by(other_value)
    assert result.equals(
        pd.Series(
            [
                True,
                True,
                True,
                True,
                True,
            ]
        )
    )


@pytest.mark.parametrize("dataset_class", [PandasDataset])
def test_target_is_sorted_by_partial_dates(dataset_class):
    """
    Test target_is_sorted_by with partial date comparisons
    """
    partial_date_df = dataset_class.from_dict(
        {
            "USUBJID": [
                "CDISC001",
                "CDISC001",
                "CDISC001",
                "CDISC002",
                "CDISC002",
                "CDISC002",
            ],
            "SESEQ": [1, 2, 3, 1, 2, 3],
            "SESTDTC": [
                "2006",
                "2006-06",
                "2006-06-15",
                "2007",
                "2007-01",
                "2007-02-01",
            ],
        }
    )

    other_value: dict = {
        "target": "--SEQ",
        "within": "USUBJID",
        "comparator": [
            {"name": "--STDTC", "sort_order": "ASC", "null_position": "last"}
        ],
    }
    result = DataframeType(
        {"value": partial_date_df, "column_prefix_map": {"--": "SE"}}
    ).target_is_sorted_by(other_value)
    assert result.equals(
        pd.Series(
            [
                False,
                False,
                True,
                False,
                True,
                True,
            ]
        )
    )


@pytest.mark.parametrize(
    "target, comparator, dataset_type, expected_result",
    [
        ("TESTID", "TESTNAME", PandasDataset, [True, False, True, False]),
        ("TESTID", "TESTNAME", DaskDataset, [True, False, True, False]),
        ("TESTNAME", "TESTID", PandasDataset, [True, False, True, False]),
        ("TESTNAME", "TESTID", DaskDataset, [True, False, True, False]),
    ],
)
def test_is_unique_relationship(target, comparator, dataset_type, expected_result):
    data = {
        "STUDYID": [
            "TEST",
            "TEST-1",
            "TEST-2",
            "TEST-3",
        ],
        "TESTID": [1, 2, 1, 3],
        "TESTNAME": [
            "Functional",
            "Stress",
            "Functional",
            "Stress",
        ],
    }
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df}).is_unique_relationship(
        {"target": target, "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "target, order, dataset_type, data, expected_result",
    [
        (
            "AESEQ",
            "asc",
            PandasDataset,
            {
                "USUBJID": [
                    "a",
                    "a",
                    "a",
                    "a",
                    "a",
                ],
                "AESEQ": [
                    "a",
                    "b",
                    "c",
                    "d",
                    "e",
                ],
            },
            [True, True, True, True, True],
        ),
        (
            "AESEQ",
            "asc",
            DaskDataset,
            {
                "USUBJID": [
                    "a",
                    "a",
                    "a",
                    "a",
                    "a",
                ],
                "AESEQ": [
                    "a",
                    "b",
                    "c",
                    "d",
                    "e",
                ],
            },
            [True, True, True, True, True],
        ),
        (
            "AESEQ",
            "dsc",
            PandasDataset,
            {
                "USUBJID": [
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                ],
                "AESEQ": [
                    "2020-02-27",
                    "2020-02-26",
                    "2020-02-25",
                    "2020-02-24",
                    "2020-02-23",
                ],
            },
            [True, True, True, True, True],
        ),
        (
            "AESEQ",
            "dsc",
            DaskDataset,
            {
                "USUBJID": [
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                ],
                "AESEQ": [
                    "2020-02-27",
                    "2020-02-26",
                    "2020-02-25",
                    "2020-02-24",
                    "2020-02-23",
                ],
            },
            [True, True, True, True, True],
        ),
        (
            "AESEQ",
            "dsc",
            PandasDataset,
            {
                "USUBJID": [
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                ],
                "AESEQ": [
                    "2020-02-24",
                    "2020-02-25",
                    "2020-02-27",
                    "2020-02-26",
                    "2020-02-23",
                ],
            },
            [False, False, False, False, True],
        ),
        (
            "AESEQ",
            "dsc",
            DaskDataset,
            {
                "USUBJID": [
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                ],
                "AESEQ": [
                    "2020-02-24",
                    "2020-02-25",
                    "2020-02-27",
                    "2020-02-26",
                    "2020-02-23",
                ],
            },
            [False, False, False, False, True],
        ),
    ],
)
def test_is_ordered_by(target, order, dataset_type, data, expected_result):
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df}).is_ordered_by(
        {"target": target, "order": order}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "target, order, dataset_type, data, expected_result",
    [
        (
            "AESEQ",
            "asc",
            PandasDataset,
            {
                "USUBJID": [
                    "a",
                    "a",
                    "a",
                    "a",
                    "a",
                ],
                "AESEQ": [
                    "a",
                    "b",
                    "c",
                    "d",
                    "e",
                ],
            },
            [False, False, False, False, False],
        ),
        (
            "AESEQ",
            "asc",
            DaskDataset,
            {
                "USUBJID": [
                    "a",
                    "a",
                    "a",
                    "a",
                    "a",
                ],
                "AESEQ": [
                    "a",
                    "b",
                    "c",
                    "d",
                    "e",
                ],
            },
            [False, False, False, False, False],
        ),
        (
            "AESEQ",
            "dsc",
            PandasDataset,
            {
                "USUBJID": [
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                ],
                "AESEQ": [
                    "2020-02-27",
                    "2020-02-26",
                    "2020-02-25",
                    "2020-02-24",
                    "2020-02-23",
                ],
            },
            [False, False, False, False, False],
        ),
        (
            "AESEQ",
            "dsc",
            DaskDataset,
            {
                "USUBJID": [
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                ],
                "AESEQ": [
                    "2020-02-27",
                    "2020-02-26",
                    "2020-02-25",
                    "2020-02-24",
                    "2020-02-23",
                ],
            },
            [False, False, False, False, False],
        ),
        (
            "AESEQ",
            "dsc",
            PandasDataset,
            {
                "USUBJID": [
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                ],
                "AESEQ": [
                    "2020-02-24",
                    "2020-02-25",
                    "2020-02-27",
                    "2020-02-26",
                    "2020-02-23",
                ],
            },
            [True, True, True, True, False],
        ),
        (
            "AESEQ",
            "dsc",
            DaskDataset,
            {
                "USUBJID": [
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                    "2020-02-23",
                ],
                "AESEQ": [
                    "2020-02-24",
                    "2020-02-25",
                    "2020-02-27",
                    "2020-02-26",
                    "2020-02-23",
                ],
            },
            [True, True, True, True, False],
        ),
    ],
)
def test_is_not_ordered_by(target, order, dataset_type, data, expected_result):
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df}).is_not_ordered_by(
        {"target": target, "order": order}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "dataset_type",
    [
        PandasDataset,
        DaskDataset,
    ],
)
def test_value_has_multiple_references(dataset_type):
    data = {
        "LNKGRP": ["A", "B", "A", "A", "A"],
        "$VALUE_COUNTS": [
            {"A": 2, "B": 1},
            {"A": 2, "B": 1},
            {"A": 2, "B": 1},
            {"A": 2, "B": 1},
            {"A": 2, "B": 1},
        ],
    }
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df}).value_has_multiple_references(
        {"target": "LNKGRP", "comparator": "$VALUE_COUNTS"}
    )
    assert result.equals(df.convert_to_series([True, False, True, True, True]))


@pytest.mark.parametrize(
    "dataset_type",
    [
        PandasDataset,
        DaskDataset,
    ],
)
def test_value_does_not_have_multiple_references(dataset_type):
    data = {
        "LNKGRP": ["A", "B", "A", "A", "A"],
        "$VALUE_COUNTS": [
            {"A": 2, "B": 1},
            {"A": 2, "B": 1},
            {"A": 2, "B": 1},
            {"A": 2, "B": 1},
            {"A": 2, "B": 1},
        ],
    }
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df}).value_does_not_have_multiple_references(
        {"target": "LNKGRP", "comparator": "$VALUE_COUNTS"}
    )
    assert result.equals(df.convert_to_series([False, True, False, False, False]))
