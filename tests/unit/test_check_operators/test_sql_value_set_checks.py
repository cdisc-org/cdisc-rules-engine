import pytest

from .helpers import create_sql_operators, assert_series_equals


@pytest.mark.parametrize(
    "target, comparator, expected_result",
    [
        ("BGSTRESU", "USUBJID", [False, False, True, True]),
        ("STRESU", "TESTCD", [True, True, True, False]),
        ("STRESU", ["TESTCD", "METHOD"], [False, False, False, False]),
    ],
)
def test_sql_is_inconsistent_across_dataset(target, comparator, expected_result):
    data = {
        "USUBJID": ["SUBJ1", "SUBJ1", "SUBJ2", "SUBJ2"],
        "BGSTRESU": ["kg", "kg", "g", "mg"],
        "TESTCD": ["TEST1", "TEST1", "TEST1", "TEST2"],
        "METHOD": ["M1", "M1", "M2", "M2"],
        "SPEC": ["S1", "S1", "S1", "S1"],
        "STRESU": ["mg", "mg", "g", "kg"],
    }
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_inconsistent_across_dataset({"target": target, "comparator": comparator})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "target, comparator, expected_result",
    [
        ("BGSTRESU", "USUBJID", [False, False, True, True]),
        ("VSELTM", "VISITNUM", [True, True, True, True]),
        ("VSELTM", ["VISITNUM", "VSTPTNUM"], [True, True, True, True]),
    ],
)
def test_sql_is_inconsistent_across_dataset_with_nulls(target, comparator, expected_result):
    """Test case covering both NULL target values and NULL comparator values"""
    data = {
        "USUBJID": ["SUBJ1", "SUBJ1", "CDISC001", "CDISC001"],
        "BGSTRESU": ["kg", "kg", None, "g"],
        "VISITNUM": [4.0, 4.0, None, None],
        "VSELTM": ["-PT4H", "PT4H", "-PT4H", "PT4H"],
        "VSTPTNUM": [14, 14, 14, 14],
    }
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_inconsistent_across_dataset({"target": target, "comparator": comparator})
    assert_series_equals(result, expected_result)
