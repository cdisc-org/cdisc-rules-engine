from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
import pytest
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


@pytest.mark.parametrize(
    "data,dataset_type,expected_result",
    [
        (
            {"target": ["2021", "2099", "2022", "2023"]},
            PandasDataset,
            [False, False, False, False],
        ),
        (
            {"target": ["90999", "20999", "2022", "2023"]},
            DaskDataset,
            [True, True, False, False],
        ),
        (
            {
                "target": [
                    "2022-03-11T092030",
                    "2022-03-11T09,20,30",
                    "2022-03-11T09@20@30",
                    "2022-03-11T09!20:30",
                ]
            },
            PandasDataset,
            [True, True, True, True],
        ),
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "2022-05-08T13:44:66",
                ]
            },
            DaskDataset,
            [False, False, False, True],
        ),
    ],
)
def test_invalid_date(data, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.invalid_date({"target": "target"})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:20:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            PandasDataset,
            [True, False, False, False, False],
        ),
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ]
            },
            "1997-07",
            DaskDataset,
            [True, False, False, False, False],
        ),
    ],
)
def test_date_equal_to(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_equal_to(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,date_component,dataset_type,expected_result",
    [
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:20:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            "hour",
            PandasDataset,
            [True, True, True, True, True],
        ),
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:20:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            "month",
            DaskDataset,
            [True, False, False, False, False],
        ),
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:22:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:21:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            "minute",
            PandasDataset,
            [True, True, False, False, True],
        ),
    ],
)
def test_date_equal_to_date_components(
    data, comparator, date_component, dataset_type, expected_result
):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_equal_to(
        {"target": "target", "comparator": comparator, "date_component": date_component}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:20:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            PandasDataset,
            [False, True, True, True, True],
        ),
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ]
            },
            "1997-07",
            DaskDataset,
            [False, False, False, False, False],
        ),
    ],
)
def test_date_less_than(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_less_than(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,date_component,dataset_type,expected_result",
    [
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:20:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            "hour",
            PandasDataset,
            [False, False, False, False, False],
        ),
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:20:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            "month",
            DaskDataset,
            [False, True, True, True, True],
        ),
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:22:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:21:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            "minute",
            PandasDataset,
            [False, False, True, False, False],
        ),
    ],
)
def test_date_less_than_date_components(
    data, comparator, date_component, dataset_type, expected_result
):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_less_than(
        {"target": "target", "comparator": comparator, "date_component": date_component}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:20:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            PandasDataset,
            [True, True, True, True, True],
        ),
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ]
            },
            "1997-07",
            DaskDataset,
            [True, False, False, False, False],
        ),
    ],
)
def test_date_less_than_or_equal_to(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_less_than_or_equal_to(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,date_component,dataset_type,expected_result",
    [
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:20:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            "hour",
            PandasDataset,
            [True, True, True, True, True],
        ),
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:20:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            "month",
            DaskDataset,
            [True, True, True, True, True],
        ),
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:22:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:21:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            "minute",
            PandasDataset,
            [True, True, True, False, True],
        ),
    ],
)
def test_date_less_than_or_equal_to_date_components(
    data, comparator, date_component, dataset_type, expected_result
):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_less_than_or_equal_to(
        {"target": "target", "comparator": comparator, "date_component": date_component}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:20:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            DaskDataset,
            [False, False, False, False, False],
        ),
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ]
            },
            "1997-07",
            PandasDataset,
            [False, True, True, True, True],
        ),
    ],
)
def test_date_greater_than(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_greater_than(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,date_component,dataset_type,expected_result",
    [
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:20:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            "hour",
            PandasDataset,
            [False, False, False, False, False],
        ),
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:20:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            "month",
            DaskDataset,
            [False, False, False, False, False],
        ),
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:22:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:21:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            "minute",
            PandasDataset,
            [False, False, False, True, False],
        ),
    ],
)
def test_date_greater_than_date_components(
    data, comparator, date_component, dataset_type, expected_result
):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_greater_than(
        {"target": "target", "comparator": comparator, "date_component": date_component}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:20:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            DaskDataset,
            [True, False, False, False, False],
        ),
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ]
            },
            "1997-07",
            PandasDataset,
            [True, True, True, True, True],
        ),
    ],
)
def test_date_greater_than_or_equal_to(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_greater_than_or_equal_to(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,date_component, dataset_type, expected_result",
    [
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:20:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            "hour",
            PandasDataset,
            [True, True, True, True, True],
        ),
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:20:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:20:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            "month",
            DaskDataset,
            [True, False, False, False, False],
        ),
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "1997-07-16T19:22:30+01:00",
                    "1997-07-16T19:20+01:00",
                ],
                "comparator": [
                    "1997-07",
                    "1997-08-16",
                    "1997-08-16T19:21:30.45+01:00",
                    "1997-08-16T19:20:30+01:00",
                    "1997-08-16T19:20+01:00",
                ],
            },
            "comparator",
            "minute",
            PandasDataset,
            [True, True, False, True, True],
        ),
    ],
)
def test_date_greater_than_or_equal_to_date_components(
    data, comparator, date_component, dataset_type, expected_result
):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_greater_than_or_equal_to(
        {"target": "target", "comparator": comparator, "date_component": date_component}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "target, dataset_type, expected_result",
    [
        ("var1", PandasDataset, [False, False, False]),
        ("var1", DaskDataset, [False, False, False]),
        ("var2", PandasDataset, [True, True, True]),
        ("var2", DaskDataset, [True, True, True]),
    ],
)
def test_is_complete_date(target, dataset_type, expected_result):
    data = {
        "var1": ["2021", "2021", "2099"],
        "var2": ["1997-07-16", "1997-07-16T19:20:30+01:00", "1997-07-16T19:20+01:00"],
    }
    df = dataset_type.from_dict(data)
    assert (
        DataframeType({"value": df})
        .is_complete_date({"target": target})
        .equals(df.convert_to_series(expected_result))
    )


@pytest.mark.parametrize(
    "target, dataset_type, expected_result",
    [
        ("var1", PandasDataset, [True, True, True]),
        ("var1", DaskDataset, [True, True, True]),
        ("var2", PandasDataset, [False, False, False]),
        ("var2", DaskDataset, [False, False, False]),
    ],
)
def test_is_incomplete_date(target, dataset_type, expected_result):
    data = {
        "var1": ["2021", "2021", "2099"],
        "var2": ["1997-07-16", "1997-07-16T19:20:30+01:00", "1997-07-16T19:20+01:00"],
    }
    df = dataset_type.from_dict(data)
    assert (
        DataframeType({"value": df})
        .is_incomplete_date({"target": target})
        .equals(df.convert_to_series(expected_result))
    )


# Tests for automatic precision detection (date_component: "auto")


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        # Date vs datetime - should compare at day level
        (
            {"target": ["2025-06-25", "2025-06-24", "2025-06-25", "2025-06-26"]},
            "2025-06-25T17:22",
            PandasDataset,
            [True, False, True, False],
        ),
        # Partial date (year-month) vs complete date - should compare at month level
        (
            {"target": ["2025-06", "2025-07", "2025-06", "2025-05"]},
            "2025-06-25",
            DaskDataset,
            [True, False, True, False],
        ),
        # Year only vs complete date - should compare at year level
        (
            {"target": ["2025", "2024", "2025", "2026"]},
            "2025-06-25T17:22:30",
            PandasDataset,
            [True, False, True, False],
        ),
        # Both have same precision - should work normally
        (
            {"target": ["2025-06-25", "2025-06-24", "2025-06-26", "2025-06-25"]},
            "2025-06-25",
            DaskDataset,
            [True, False, False, True],
        ),
        # Datetime vs datetime with different time precision
        (
            {"target": ["2025-06-25T17:22", "2025-06-25T17:21", "2025-06-25T17:22"]},
            "2025-06-25T17:22:30",
            PandasDataset,
            [True, False, True],
        ),
    ],
)
def test_date_equal_to_auto_precision(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_equal_to(
        {"target": "target", "comparator": comparator, "date_component": "auto"}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        # Date vs datetime at day level
        (
            {"target": ["2025-06-26", "2025-06-24", "2025-06-25"]},
            "2025-06-25T17:22",
            PandasDataset,
            [True, False, False],
        ),
        # Year-month vs complete date at month level
        (
            {"target": ["2025-07", "2025-05", "2025-06"]},
            "2025-06-25",
            DaskDataset,
            [True, False, False],
        ),
        # Year only vs datetime
        (
            {"target": ["2026", "2024", "2025"]},
            "2025-06-25T17:22",
            PandasDataset,
            [True, False, False],
        ),
    ],
)
def test_date_greater_than_auto_precision(
    data, comparator, dataset_type, expected_result
):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_greater_than(
        {"target": "target", "comparator": comparator, "date_component": "auto"}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        # Date vs datetime at day level
        (
            {"target": ["2025-06-26", "2025-06-24", "2025-06-25"]},
            "2025-06-25T17:22",
            DaskDataset,
            [True, False, True],
        ),
        # Year-month vs complete date at month level
        (
            {"target": ["2025-07", "2025-05", "2025-06"]},
            "2025-06-25",
            PandasDataset,
            [True, False, True],
        ),
    ],
)
def test_date_greater_than_or_equal_to_auto_precision(
    data, comparator, dataset_type, expected_result
):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_greater_than_or_equal_to(
        {"target": "target", "comparator": comparator, "date_component": "auto"}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        # Date vs datetime at day level
        (
            {"target": ["2025-06-24", "2025-06-26", "2025-06-25"]},
            "2025-06-25T17:22",
            PandasDataset,
            [True, False, False],
        ),
        # Year-month vs complete date at month level
        (
            {"target": ["2025-05", "2025-07", "2025-06"]},
            "2025-06-25",
            DaskDataset,
            [True, False, False],
        ),
        # Year only vs datetime
        (
            {"target": ["2024", "2026", "2025"]},
            "2025-06-25T17:22",
            PandasDataset,
            [True, False, False],
        ),
    ],
)
def test_date_less_than_auto_precision(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_less_than(
        {"target": "target", "comparator": comparator, "date_component": "auto"}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        # Date vs datetime at day level
        (
            {"target": ["2025-06-24", "2025-06-26", "2025-06-25"]},
            "2025-06-25T17:22",
            DaskDataset,
            [True, False, True],
        ),
        # Year-month vs complete date at month level
        (
            {"target": ["2025-05", "2025-07", "2025-06"]},
            "2025-06-25",
            PandasDataset,
            [True, False, True],
        ),
    ],
)
def test_date_less_than_or_equal_to_auto_precision(
    data, comparator, dataset_type, expected_result
):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_less_than_or_equal_to(
        {"target": "target", "comparator": comparator, "date_component": "auto"}
    )
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        # Date vs datetime at day level
        (
            {"target": ["2025-06-24", "2025-06-25", "2025-06-26"]},
            "2025-06-25T17:22",
            PandasDataset,
            [True, False, True],
        ),
        # Year-month vs complete date at month level
        (
            {"target": ["2025-05", "2025-06", "2025-07"]},
            "2025-06-25",
            DaskDataset,
            [True, False, True],
        ),
    ],
)
def test_date_not_equal_to_auto_precision(
    data, comparator, dataset_type, expected_result
):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_not_equal_to(
        {"target": "target", "comparator": comparator, "date_component": "auto"}
    )
    assert result.equals(df.convert_to_series(expected_result))
