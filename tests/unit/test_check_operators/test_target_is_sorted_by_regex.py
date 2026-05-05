"""
Tests for target_is_sorted_by operator with regex support
"""

import pytest
import pandas as pd
from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset


@pytest.mark.parametrize("dataset_class", [PandasDataset, DaskDataset])
def test_target_is_sorted_by_with_regex_non_padded(dataset_class):
    """
    Test target_is_sorted_by with regex extraction for non-zero-padded sequence numbers.
    Example: lalala1, lalala2, ..., lalala9, lalala10
    """
    df = dataset_class.from_dict(
        {
            "USUBJID": ["001", "001", "001", "001", "002", "002", "002"],
            "MIDSTYPE": ["A", "A", "A", "A", "B", "B", "B"],
            "MIDS": [
                "lalala1",
                "lalala2",
                "lalala9",
                "lalala10",
                "test1",
                "test2",
                "test10",
            ],
            "SMSTDTC": [
                "2020-01-01",
                "2020-01-02",
                "2020-01-09",
                "2020-01-10",
                "2020-02-01",
                "2020-02-02",
                "2020-02-10",
            ],
        }
    )

    other_value = {
        "target": "MIDS",
        "regex": ".*?(\\d+)$",  # Non-greedy to correctly extract multi-digit numbers
        "within": ["USUBJID", "MIDSTYPE"],
        "comparator": [
            {"name": "SMSTDTC", "sort_order": "ASC", "null_position": "first"}
        ],
    }

    result = DataframeType({"value": df}).target_is_sorted_by(other_value)
    # All should be True - sorted correctly by chronological order
    assert result.equals(pd.Series([True, True, True, True, True, True, True]))


@pytest.mark.parametrize("dataset_class", [PandasDataset, DaskDataset])
def test_target_is_sorted_by_with_regex_zero_padded(dataset_class):
    """
    Test target_is_sorted_by with regex extraction for zero-padded sequence numbers.
    Example: lalala01, lalala02, ..., lalala09, lalala10
    """
    df = dataset_class.from_dict(
        {
            "USUBJID": ["001", "001", "001", "001"],
            "MIDSTYPE": ["A", "A", "A", "A"],
            "MIDS": ["lalala01", "lalala02", "lalala09", "lalala10"],
            "SMSTDTC": [
                "2020-01-01",
                "2020-01-02",
                "2020-01-09",
                "2020-01-10",
            ],
        }
    )

    other_value = {
        "target": "MIDS",
        "regex": ".*?(\\d+)$",  # Non-greedy to correctly extract multi-digit numbers
        "within": ["USUBJID", "MIDSTYPE"],
        "comparator": [
            {"name": "SMSTDTC", "sort_order": "ASC", "null_position": "first"}
        ],
    }

    result = DataframeType({"value": df}).target_is_sorted_by(other_value)
    # All should be True - numeric conversion handles zero-padding
    assert result.equals(pd.Series([True, True, True, True]))


@pytest.mark.parametrize("dataset_class", [PandasDataset, DaskDataset])
def test_target_is_sorted_by_with_regex_invalid_order(dataset_class):
    """
    Test that invalid order is detected even with regex extraction.
    """
    df = dataset_class.from_dict(
        {
            "USUBJID": ["001", "001", "001", "001"],
            "MIDSTYPE": ["A", "A", "A", "A"],
            "MIDS": ["lalala1", "lalala10", "lalala2", "lalala9"],
            "SMSTDTC": [
                "2020-01-01",
                "2020-01-02",
                "2020-01-09",
                "2020-01-10",
            ],
        }
    )

    other_value = {
        "target": "MIDS",
        "regex": ".*?(\\d+)$",  # Non-greedy to correctly extract multi-digit numbers
        "within": ["USUBJID", "MIDSTYPE"],
        "comparator": [
            {"name": "SMSTDTC", "sort_order": "ASC", "null_position": "first"}
        ],
    }

    result = DataframeType({"value": df}).target_is_sorted_by(other_value)
    # After sorting by extracted MIDS (1, 2, 9, 10), dates should be:
    # MIDS=1 (2020-01-01) -> MIDS=2 (should be 2020-01-02) -> MIDS=9 (should be 2020-01-09) -> MIDS=10 (should be 2020-01-10)
    # Actual dates: 2020-01-01, 2020-01-09, 2020-01-10, 2020-01-02
    # Only MIDS=1 is in correct chronological position
    assert result.equals(pd.Series([True, False, False, False]))


@pytest.mark.parametrize("dataset_class", [PandasDataset, DaskDataset])
def test_target_is_sorted_by_with_regex_multiple_groups(dataset_class):
    """
    Test regex sorting with multiple USUBJID and MIDSTYPE groups.
    """
    df = dataset_class.from_dict(
        {
            "USUBJID": ["001", "001", "001", "002", "002", "002"],
            "MIDSTYPE": ["A", "A", "A", "A", "A", "A"],
            "MIDS": ["M1", "M2", "M3", "M1", "M2", "M3"],
            "SMSTDTC": [
                "2020-01-01",
                "2020-01-02",
                "2020-01-03",
                "2020-02-01",
                "2020-02-02",
                "2020-02-03",
            ],
        }
    )

    other_value = {
        "target": "MIDS",
        "regex": ".*?(\\d+)$",  # Non-greedy to correctly extract multi-digit numbers
        "within": ["USUBJID", "MIDSTYPE"],
        "comparator": [
            {"name": "SMSTDTC", "sort_order": "ASC", "null_position": "first"}
        ],
    }

    result = DataframeType({"value": df}).target_is_sorted_by(other_value)
    assert result.equals(pd.Series([True, True, True, True, True, True]))


@pytest.mark.parametrize("dataset_class", [PandasDataset, DaskDataset])
def test_target_is_sorted_by_without_regex_still_works(dataset_class):
    """
    Test that the operator still works without regex (backward compatibility).
    """
    df = dataset_class.from_dict(
        {
            "USUBJID": ["001", "001", "001"],
            "SESEQ": [1, 2, 3],
            "SESTDTC": [
                "2020-01-01",
                "2020-01-02",
                "2020-01-03",
            ],
        }
    )

    other_value = {
        "target": "SESEQ",
        "within": "USUBJID",
        "comparator": [{"name": "SESTDTC", "sort_order": "ASC"}],
    }

    result = DataframeType({"value": df}).target_is_sorted_by(other_value)
    assert result.equals(pd.Series([True, True, True]))
