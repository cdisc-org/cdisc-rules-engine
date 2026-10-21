"""Columns named after reserved SQL keywords (ORDER, GROUP, USER...) can be loaded and checked like any other."""

from .helpers import assert_series_equals, create_sql_operators

DATA = {"ORDER": ["A", "", "B"], "GROUP": ["A", "G2", "C"], "USER": ["u1", "u2", "u3"]}


def test_empty_on_reserved_word_column():
    sql_ops = create_sql_operators(DATA)
    assert_series_equals(sql_ops.empty({"target": "ORDER"}), [False, True, False])


def test_comparing_two_reserved_word_columns():
    sql_ops = create_sql_operators(DATA)
    result = sql_ops.equal_to({"target": "ORDER", "comparator": "GROUP", "value_is_literal": False})
    assert_series_equals(result, [True, False, False])


def test_comparing_reserved_word_column_with_a_literal():
    sql_ops = create_sql_operators(DATA)
    result = sql_ops.equal_to({"target": "USER", "comparator": "u2", "value_is_literal": True})
    assert_series_equals(result, [False, True, False])
