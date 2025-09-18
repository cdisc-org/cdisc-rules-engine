import pytest
from .helpers import create_sql_operators, assert_series_equals


@pytest.mark.parametrize(
    "data,expected_result",
    [
        (
            {"target": ["P1Y", "P1M", "P1D", "PT1H"]},
            [False, False, False, False],
        ),
        (
            {"target": ["P1Y2M3D", "P1DT2H3M", "PT1H30M", "P1W"]},
            [False, False, False, False],
        ),
        (
            {"target": ["P1Y1M1DT1H1M1.5S", "PT1.5S", "P1DT1H1M1.123S", "PT24H"]},
            [False, False, False, False],
        ),
        (
            {"target": ["P1.5Y", "P2,5M", "PT1.5H", "PT30,5S"]},
            [False, False, False, False],
        ),
        (
            {"target": ["P4W", "P1.5W", "P1Y,5M", "P1Y2M"]},
            [False, False, False, False],
        ),
        (
            {"target": ["P", "1Y", "PT", "P1S"]},
            [True, True, True, True],
        ),
        (
            {"target": ["P1YT", "P-1Y", "P1M2D3H", "P1D1H"]},
            [True, True, True, True],
        ),
        (
            {"target": ["P1Y2M3W4DT5H6M7.89S", "P1Y2M3DT", "P 1Y", "P1Y1W"]},
            [True, True, True, True],
        ),
        (
            {"target": ["P1Y2M3.4D5H", "PT1H2M3.4S5M", "random_string"]},
            [True, True, True],
        ),
        (
            {"target": ["P1.5.2Y", "P1,2,3M", "PT1.5.2H", "PT30,5,1S"]},
            [True, True, True, True],
        ),
        (
            {"target": ["P1.5Y2,3M", "P1,2Y3.4M", "PT1.5H2,3M", "P1,5Y2M3DT4,5H5M6,7S"]},
            [True, True, True, True],
        ),
        (
            {"target": [None, None, None]},
            [False, False, False],
        ),
        (
            {"target": ["P1Y", "invalid", None, "PT1H", "P"]},
            [False, True, False, False, True],
        ),
        (
            {"target": ["PT0.1S", "PT0,1S", "P0D", "P1M2.5D", "P1Y2.5M"]},
            [False, False, False, False, False],
        ),
        (
            {"target": ["P0Y", "P0M", "P0D", "PT0H", "PT0M", "PT0S"]},
            [False, False, False, False, False, False],
        ),
        (
            {"target": ["P0Y0M0D", "PT0H0M0S", "P0W"]},
            [False, False, False],
        ),
    ],
)
def test_invalid_duration_without_negative(data, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.invalid_duration({"target": "target"})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,expected_result",
    [
        (
            {"target": ["P1Y", "P2M", "P3D", "PT1H", "PT30M", "PT45S"]},
            [False, False, False, False, False, False],
        ),
        (
            {"target": ["-P1Y", "-P2M", "-P3D", "-PT1H", "-PT30M", "-PT45S"]},
            [False, False, False, False, False, False],
        ),
        (
            {"target": ["-P1Y2M3D", "-P1Y2M3DT4H5M6S", "-P1W", "-P2W"]},
            [False, False, False, False],
        ),
        (
            {"target": ["-P1.5Y", "-P2,5M", "-PT1.5H", "-PT30,5S"]},
            [False, False, False, False],
        ),
        (
            {"target": ["P", "PT", "-P", "-PT", "1Y", "-1Y"]},
            [True, True, True, True, True, True],
        ),
        (
            {"target": ["-P1Y2M3DT", "-PT1H2M3ST", "random_string"]},
            [True, True, True],
        ),
        (
            {"target": [None, None, None]},
            [False, False, False],
        ),
        (
            {"target": ["P1Y", "-P1Y", "invalid", None, "-PT1H", "P"]},
            [False, False, True, False, False, True],
        ),
        (
            {"target": ["-P0Y", "-P0M", "-P0D", "-PT0H", "-PT0M", "-PT0S"]},
            [False, False, False, False, False, False],
        ),
    ],
)
def test_invalid_duration_with_negative(data, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.invalid_duration({"target": "target", "negative": True})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,expected_result",
    [
        (
            {"target": ["-P1Y", "-P2M", "-P3D", "-PT1H", "-PT30M", "-PT45S"]},
            [True, True, True, True, True, True],
        ),
        (
            {"target": ["-P1Y2M3D", "-P1Y2M3DT4H5M6S", "-P1W"]},
            [True, True, True],
        ),
        (
            {"target": ["P1Y", "-P1Y", "P2M", "-P2M"]},
            [False, True, False, True],
        ),
    ],
)
def test_invalid_duration_negative_durations_without_flag(data, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.invalid_duration({"target": "target"})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,negative,expected_result",
    [
        (
            {"target": ["P1Y", "P1M", "P1D", "PT1H"]},
            False,
            [False, False, False, False],
        ),
        (
            {"target": ["P", "1Y", "PT", "P1S"]},
            False,
            [True, True, True, True],
        ),
        (
            {"target": ["-P1Y", "P1M", "P1D", "-PT1H", "P1Y2M", "-P1.5D", "P1Y,5M"]},
            True,
            [False, False, False, False, False, False, False],
        ),
        (
            {"target": ["-P1Y", "P1M", "-PT1H", "P1D"]},
            False,
            [True, False, True, False],
        ),
        (
            {"target": ["P1Y2M3DT4H5M6S", "-P1Y2M3DT4H5M6S", "P", "-P"]},
            False,
            [False, True, True, True],
        ),
        (
            {"target": ["P1Y2M3DT4H5M6S", "-P1Y2M3DT4H5M6S", "P", "-P"]},
            True,
            [False, False, True, True],
        ),
    ],
)
def test_invalid_duration_with_negative_parameter(data, negative, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.invalid_duration({"target": "target", "negative": negative})
    assert_series_equals(result, expected_result)


def test_invalid_duration_mixed_decimal_separators():
    sql_ops = create_sql_operators({"target": ["P1,5Y2M3DT4,5H5M6,7S", "P1.5Y2,3M", "P1,2Y3.4M"]})
    result = sql_ops.invalid_duration({"target": "target"})
    assert_series_equals(result, [True, True, True])


def test_invalid_duration_specific_failing_case():
    sql_ops = create_sql_operators({"target": ["P1Y2M3DT4H5M6S", "P1,5Y2M3DT4,5H5M6,7S"]})
    result = sql_ops.invalid_duration({"target": "target"})
    assert_series_equals(result, [False, True])


def test_invalid_duration_comprehensive_edge_cases():
    data = {
        "target": [
            "P1Y2M3W4DT5H6M7.89S",
            "PT0.1S",
            "PT0,1S",
            "P0D",
            "P1Y2M3DT",
            "P1.5Y",
            "P1M2.5D",
            "P 1Y",
            "P1Y2.5M",
            "P1.5W",
            "P1Y,5M",
            "P1Y2M3.4D5H",
            "PT1H2M3.4S5M",
            "P4W",
            "P1Y1W",
        ]
    }
    sql_ops = create_sql_operators(data)
    result = sql_ops.invalid_duration({"target": "target"})
    expected = [
        True,
        False,
        False,
        False,
        True,
        False,
        False,
        True,
        False,
        False,
        False,
        True,
        True,
        False,
        True,
    ]
    assert_series_equals(result, expected)


def test_invalid_duration_null_handling_difference():
    sql_ops = create_sql_operators({"target": [None, "P1Y", None, "invalid"]})
    result = sql_ops.invalid_duration({"target": "target"})
    assert_series_equals(result, [False, False, False, True])


@pytest.mark.parametrize(
    "data,negative,operator,expected_result",
    [
        (
            {"target": ["P1Y", "P1M"]},
            False,
            "invalid_duration",
            [False, False],
        ),
        (
            {"target": ["P1Y", "P1M"]},
            True,
            "invalid_duration",
            [False, False],
        ),
        (
            {"target": ["-P1Y", "-P1M"]},
            False,
            "invalid_duration",
            [True, True],
        ),
        (
            {"target": ["-P1Y", "-P1M"]},
            True,
            "invalid_duration",
            [False, False],
        ),
        (
            {"target": ["P1Y", None, "-P1M"]},
            False,
            "invalid_duration",
            [False, False, True],
        ),
        (
            {"target": ["P1Y", None, "-P1M"]},
            True,
            "invalid_duration",
            [False, False, False],
        ),
    ],
)
def test_invalid_duration_operators(data, negative, operator, expected_result):
    sql_ops = create_sql_operators(data)
    if operator == "invalid_duration":
        result = sql_ops.invalid_duration({"target": "target", "negative": negative})
    assert_series_equals(result, expected_result)
