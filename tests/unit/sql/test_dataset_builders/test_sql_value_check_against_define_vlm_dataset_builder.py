from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from cdisc_rules_engine.models.define.value_level_metadata import ValueLevelMetadata
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_value_check_against_define_vlm_dataset_builder import (
    SOURCE_CTE,
    SqlValueCheckAgainstDefineVLMDatasetBuilder as Builder,
    UntranslatableWhereClause,
)

COL = "c"


def range_check(comparator, values, column_type="Char"):
    return Builder._range_check_to_sql(COL, column_type, comparator, values)


def source_schema():
    schema = SqlTableSchema("lb", "lb_hash", "data")
    for name, type in [("LBTESTCD", "Char"), ("LBSTRESN", "Num"), ("LBORRES", "Char")]:
        schema.add_column(SqlColumnSchema(name=name.lower(), hash=name.lower(), type=type))
    return schema


@pytest.mark.parametrize(
    "comparator, expected",
    [("LT", "<"), ("LE", "<="), ("GT", ">"), ("GE", ">=")],
)
def test_ordering_comparators_on_numeric_column(comparator, expected):
    assert range_check(comparator, ["5"], "Num") == f"c {expected} 5.0"


def test_ordering_comparator_on_text_column_coalesces_missing():
    assert range_check("GT", ["B"]) == "COALESCE(NULLIF(CAST(c AS TEXT), '') > 'B', FALSE)"


def test_ordering_comparator_with_non_numeric_value_on_numeric_column_is_untranslatable():
    with pytest.raises(UntranslatableWhereClause):
        range_check("LT", ["abc"], "Num")


def test_eq_uses_only_first_value_and_never_matches_null():
    assert range_check("EQ", ["A", "B"]) == "COALESCE(COALESCE(CAST(c AS TEXT), '') IN ('A'), FALSE)"


def test_ne_and_notin_are_negations_that_treat_missing_as_not_equal():
    assert range_check("NE", ["A"]) == "NOT COALESCE(COALESCE(CAST(c AS TEXT), '') IN ('A'), FALSE)"
    assert range_check("NOTIN", ["A", "B"]) == "NOT COALESCE(COALESCE(CAST(c AS TEXT), '') IN ('A', 'B'), FALSE)"


def test_in_escapes_single_quotes():
    assert "'O''Brien'" in range_check("IN", ["O'Brien"])


def test_numeric_column_compares_numerically_so_5_equals_5_0():
    assert range_check("EQ", ["5"], "Num") == "COALESCE(c IN (5.0), FALSE)"
    assert range_check("IN", ["5", "6.5"], "Num") == "COALESCE(c IN (5.0, 6.5), FALSE)"


def test_numeric_column_ignores_non_numeric_values_for_equality():
    assert range_check("IN", ["5", "abc"], "Num") == "COALESCE(c IN (5.0), FALSE)"
    assert range_check("EQ", ["abc"], "Num") == "COALESCE(FALSE, FALSE)"
    assert range_check("NE", ["abc"], "Num") == "NOT COALESCE(FALSE, FALSE)"


def test_empty_value_on_numeric_column_matches_missing():
    assert range_check("EQ", [""], "Num") == "COALESCE(c IS NULL, FALSE)"
    assert range_check("NE", [""], "Num") == "NOT COALESCE(c IS NULL, FALSE)"


def test_unsupported_comparator_and_missing_values_are_untranslatable():
    with pytest.raises(UntranslatableWhereClause):
        range_check("CONTAINS", ["A"])
    with pytest.raises(UntranslatableWhereClause):
        range_check("EQ", [])


def test_where_clause_ands_every_range_check():
    sql = Builder._where_clause_to_sql(
        [
            {"variable": "LBTESTCD", "comparator": "EQ", "values": ["GLUC"]},
            {"variable": "LBSTRESN", "comparator": "GT", "values": ["5"]},
        ],
        source_schema(),
    )
    assert sql == (
        f"(COALESCE(COALESCE(CAST({SOURCE_CTE}.lbtestcd AS TEXT), '') IN ('GLUC'), FALSE))"
        f" AND ({SOURCE_CTE}.lbstresn > 5.0)"
    )


@pytest.mark.parametrize(
    "where_clause",
    [[], None, [{"variable": "NOPE", "comparator": "EQ", "values": ["A"]}]],
)
def test_where_clause_that_cannot_be_translated_raises(where_clause):
    with pytest.raises(UntranslatableWhereClause):
        Builder._where_clause_to_sql(where_clause, source_schema())


def test_literal_maps_only_none_and_empty_to_null():
    assert Builder._literal(None, "Char") == "CAST(NULL AS TEXT)"
    assert Builder._literal("", "Num") == "CAST(NULL AS NUMERIC)"
    assert Builder._literal("None", "Char") == "CAST('None' AS TEXT)"
    assert Builder._literal("it's", "Char") == "CAST('it''s' AS TEXT)"
    assert Builder._literal("true", "Bool") == "CAST('true' AS BOOLEAN)"


def vlm(variable="LBORRES", where=None):
    if where is None:
        where = [{"variable": "LBTESTCD", "comparator": "EQ", "values": ["GLUC"]}]
    return {"define_variable_name": variable, "define_vlm_name": "n", "where_clause": where}


def test_vlm_select_filters_the_shared_cte():
    sql = Builder._vlm_select(vlm(), source_schema(), [])
    assert sql.startswith("SELECT vlm_source_row_number, CAST('LBORRES' AS TEXT)")
    assert f"CAST({SOURCE_CTE}.lborres AS TEXT)" in sql
    assert f"FROM {SOURCE_CTE} WHERE " in sql


def test_vlm_select_skips_and_warns_for_unknown_variable_or_untranslatable_where(monkeypatch):
    warnings = []
    monkeypatch.setattr(
        "cdisc_rules_engine.sql_dataset_builders.sql_value_check_against_define_vlm_dataset_builder.logger.warning",
        warnings.append,
    )
    assert Builder._vlm_select(vlm(variable="NOPE"), source_schema(), []) is None
    assert Builder._vlm_select(vlm(where=[]), source_schema(), []) is None
    assert len(warnings) == 2


def test_source_columns_skip_id_derived_and_clashing_columns(monkeypatch):
    warnings = []
    monkeypatch.setattr(
        "cdisc_rules_engine.sql_dataset_builders.sql_value_check_against_define_vlm_dataset_builder.logger.warning",
        warnings.append,
    )
    schema = source_schema()
    schema.add_column(SqlColumnSchema(name="variable_name", hash="vn", type="Char"))
    schema.add_column(SqlColumnSchema(name="flag", hash="flag", type="Bool", origin="co"))

    columns = Builder._get_source_columns(schema, {"variable_name": "Char"})

    assert [name for name, _ in columns] == ["lbtestcd", "lbstresn", "lborres"]
    assert len(warnings) == 1


def test_build_computes_the_numbered_source_once_for_all_vlms():
    builder = Builder.__new__(Builder)
    builder.dataset_metadata = SimpleNamespace(name="LB")
    builder.rule = {}
    builder.standards_context = None
    pgi = MagicMock()
    pgi.schema.get_table.side_effect = lambda name: source_schema() if name == "src" else None
    pgi.schema.get_table_hash.return_value = "src_hash"
    builder.data_service = MagicMock(pgi=pgi)
    builder.data_service.get_dataset_for_rule.return_value = "src"
    builder.get_define_vlms = lambda: [vlm("LBORRES"), vlm("LBSTRESN"), vlm("MISSING")]

    assert builder.build() == "LB_value_check_define_vlm"

    (query,) = pgi.execute_sql.call_args.args
    assert query.count("ROW_NUMBER() OVER") == 1
    assert query.count("UNION ALL") == 1


def test_build_without_translatable_vlms_creates_an_empty_table():
    builder = Builder.__new__(Builder)
    builder.dataset_metadata = SimpleNamespace(name="LB")
    builder.rule = {}
    builder.standards_context = None
    pgi = MagicMock()
    pgi.schema.get_table.side_effect = lambda name: source_schema() if name == "src" else None
    builder.data_service = MagicMock(pgi=pgi)
    builder.data_service.get_dataset_for_rule.return_value = "src"
    builder.get_define_vlms = lambda: [vlm(where=[])]

    builder.build()

    pgi.create_table.assert_called_once()
    pgi.execute_sql.assert_not_called()


def make_where_clause(*range_checks):
    return SimpleNamespace(
        OID="WC.1",
        RangeCheck=[
            SimpleNamespace(
                ItemOID=item_oid,
                Comparator=comparator,
                CheckValue=[SimpleNamespace(_content=v) for v in values],
                SoftHard="Soft",
            )
            for item_oid, comparator, values in range_checks
        ],
    )


def test_value_level_metadata_get_where_clause_returns_every_range_check():
    items = {"IT.A": SimpleNamespace(Name="LBTESTCD"), "IT.B": SimpleNamespace(Name="LBSTRESN")}
    where = make_where_clause(("IT.A", "IN", ["GLUC", "NA"]), ("IT.B", "GT", ["5"]))

    result = ValueLevelMetadata.from_where_clause_def(where, items).get_where_clause()

    assert result == [
        {"variable": "LBTESTCD", "comparator": "IN", "values": ["GLUC", "NA"]},
        {"variable": "LBSTRESN", "comparator": "GT", "values": ["5"]},
    ]


def test_value_level_metadata_get_where_clause_is_empty_when_an_item_is_not_in_the_define():
    items = {"IT.A": SimpleNamespace(Name="LBTESTCD")}
    where = make_where_clause(("IT.A", "EQ", ["GLUC"]), ("IT.MISSING", "EQ", ["x"]))

    assert ValueLevelMetadata.from_where_clause_def(where, items).get_where_clause() == []
