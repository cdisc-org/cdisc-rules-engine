from cdisc_rules_engine.rule_operators.dataframe_operators import DataframeType
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "data,expected_result",
    [
        ({"target": ["2021", "2099", "2022", "2023"]}, [False, False, False, False]),
        ({"target": ["90999", "20999", "2022", "2023"]}, [True, True, False, False]),
        (
            {
                "target": [
                    "2022-03-11T092030",
                    "2022-03-11T09,20,30",
                    "2022-03-11T09@20@30",
                    "2022-03-11T09!20:30",
                ]
            },
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
            [False, False, False, True],
        ),
    ],
)
def test_invalid_date(data, expected_result):
    df = pd.DataFrame.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.invalid_date({"target": "target"})
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
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
            [True, False, False, False, False],
        ),
    ],
)
def test_date_equal_to(data, comparator, expected_result):
    df = pd.DataFrame.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_equal_to(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,date_component,expected_result",
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
            [True, True, False, False, True],
        ),
    ],
)
def test_date_equal_to_date_components(
    data, comparator, date_component, expected_result
):
    df = pd.DataFrame.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_equal_to(
        {"target": "target", "comparator": comparator, "date_component": date_component}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
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
            [False, False, False, False, False],
        ),
    ],
)
def test_date_less_than(data, comparator, expected_result):
    df = pd.DataFrame.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_less_than(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,date_component,expected_result",
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
            [False, False, True, False, False],
        ),
    ],
)
def test_date_less_than_date_components(
    data, comparator, date_component, expected_result
):
    df = pd.DataFrame.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_less_than(
        {"target": "target", "comparator": comparator, "date_component": date_component}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
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
            [True, False, False, False, False],
        ),
    ],
)
def test_date_less_than_or_equal_to(data, comparator, expected_result):
    df = pd.DataFrame.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_less_than_or_equal_to(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,date_component,expected_result",
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
            [True, True, True, False, True],
        ),
    ],
)
def test_date_less_than_or_equal_to_date_components(
    data, comparator, date_component, expected_result
):
    df = pd.DataFrame.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_less_than_or_equal_to(
        {"target": "target", "comparator": comparator, "date_component": date_component}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
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
            [False, True, True, True, True],
        ),
    ],
)
def test_date_greater_than(data, comparator, expected_result):
    df = pd.DataFrame.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_greater_than(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,date_component,expected_result",
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
            [False, False, False, True, False],
        ),
    ],
)
def test_date_greater_than_date_components(
    data, comparator, date_component, expected_result
):
    df = pd.DataFrame.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_greater_than(
        {"target": "target", "comparator": comparator, "date_component": date_component}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
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
            [True, True, True, True, True],
        ),
    ],
)
def test_date_greater_than_or_equal_to(data, comparator, expected_result):
    df = pd.DataFrame.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_greater_than_or_equal_to(
        {"target": "target", "comparator": comparator}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,date_component,expected_result",
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
            [True, True, False, True, True],
        ),
    ],
)
def test_date_greater_than_or_equal_to_date_components(
    data, comparator, date_component, expected_result
):
    df = pd.DataFrame.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.date_greater_than_or_equal_to(
        {"target": "target", "comparator": comparator, "date_component": date_component}
    )
    assert result.equals(pd.Series(expected_result))
