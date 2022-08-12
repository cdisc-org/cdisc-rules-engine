from unittest.mock import Mock

import pandas
import pytest
from odmlib.define_2_1.model import CheckValue

from cdisc_rules_engine.models.define import ValueLevelMetadata


@pytest.mark.parametrize(
    "data, expected_result",
    [
        ({"TEST": ["test"]}, False),
        ({"TEST": ["2019"]}, True),
        ({"TEST": ["2020-10"]}, True),
        ({"TEST": ["2020-10-11"]}, True),
        ({"TEST": [""]}, True),
    ],
)
def test_partial_date(data, expected_result):
    df = pandas.DataFrame.from_dict(data)
    vlm = ValueLevelMetadata()
    vlm.item = Mock(Name="TEST")
    partial_date_function = vlm.is_partial_date()
    assert partial_date_function(df.iloc[0]) == expected_result


@pytest.mark.parametrize(
    "data, expected_result",
    [
        ({"TEST": ["test"]}, False),
        ({"TEST": ["P1Y2M3W"]}, True),
        ({"TEST": [""]}, True),
    ],
)
def test_duration(data, expected_result):
    df = pandas.DataFrame.from_dict(data)
    vlm = ValueLevelMetadata()
    vlm.item = Mock(Name="TEST")
    partial_function = vlm.is_duration_datetime()
    assert partial_function(df.iloc[0]) == expected_result


@pytest.mark.parametrize(
    "data, check_value, expected_result",
    [
        ({"TEST": ["test"]}, "test", True),
        ({"TEST": [24]}, "24", True),
        ({"TEST": ["test"]}, "not_test", False),
    ],
)
def test_eq(data, check_value, expected_result):
    df = pandas.DataFrame.from_dict(data)
    vlm = ValueLevelMetadata()
    vlm.item = Mock(Name="TEST")
    vlm.check_values = [CheckValue(_content=check_value)]
    partial_function = vlm.equal_to()
    assert partial_function(df.iloc[0]) == expected_result


@pytest.mark.parametrize(
    "data, check_value, expected_result",
    [
        ({"TEST": ["test"]}, "test", False),
        ({"TEST": [24]}, "24", False),
        ({"TEST": ["test"]}, "not_test", True),
    ],
)
def test_not_eq(data, check_value, expected_result):
    df = pandas.DataFrame.from_dict(data)
    vlm = ValueLevelMetadata()
    vlm.item = Mock(Name="TEST")
    vlm.check_values = [CheckValue(_content=check_value)]
    partial_function = vlm.not_equal_to()
    assert partial_function(df.iloc[0]) == expected_result


@pytest.mark.parametrize(
    "data, check_values, expected_result",
    [
        ({"TEST": ["test"]}, ["test", "not_test", "word"], True),
        ({"TEST": [24]}, ["24", "25", "26"], True),
        ({"TEST": ["test"]}, ["not_test", "word"], False),
    ],
)
def test_is_in(data, check_values, expected_result):
    df = pandas.DataFrame.from_dict(data)
    vlm = ValueLevelMetadata()
    vlm.item = Mock(Name="TEST")
    vlm.check_values = [
        CheckValue(_content=check_value) for check_value in check_values
    ]
    partial_function = vlm.is_in()
    assert partial_function(df.iloc[0]) == expected_result


@pytest.mark.parametrize(
    "data, check_values, expected_result",
    [
        ({"TEST": ["test"]}, ["test", "not_test", "word"], False),
        ({"TEST": [24]}, ["24", "25", "26"], False),
        ({"TEST": ["test"]}, ["not_test", "word"], True),
    ],
)
def test_is_not_in(data, check_values, expected_result):
    df = pandas.DataFrame.from_dict(data)
    vlm = ValueLevelMetadata()
    vlm.item = Mock(Name="TEST")
    vlm.check_values = [
        CheckValue(_content=check_value) for check_value in check_values
    ]
    partial_function = vlm.is_not_in()
    assert partial_function(df.iloc[0]) == expected_result
