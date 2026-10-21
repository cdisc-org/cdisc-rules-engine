import pytest

from cdisc_rules_engine.data_service.reserved_keywords import SQL_RESERVED_KEYWORDS
from cdisc_rules_engine.data_service.sql_serialiser import SQLSerialiser
from cdisc_rules_engine.models.dataset_metadata2 import VariableMetadata
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema


def _variable(name, type="Char"):
    return VariableMetadata(name=name, label="", type=type, length=8, format="", order=1)


@pytest.mark.parametrize("name", ["ORDER", "group", "User", "END", "LIMIT", "WINDOW"])
def test_reserved_word_columns_get_a_generated_identifier(name):
    for column in (SqlColumnSchema.from_metadata(_variable(name)), SqlColumnSchema.from_data(name, "x")):
        assert column.name == name.lower()
        assert column.hash != name.lower()
        assert column.hash.upper() not in SQL_RESERVED_KEYWORDS
        assert column.hash.startswith(f"{name.lower()}_")


def test_reserved_word_identifier_is_the_same_whatever_the_case():
    assert SqlColumnSchema.from_metadata(_variable("ORDER")).hash == SqlColumnSchema.from_data("order", "x").hash


@pytest.mark.parametrize("name", ["STUDYID", "AETERM", "AESEQ", "QNAM", "IDVAR"])
def test_other_columns_keep_their_name_as_identifier(name):
    assert SqlColumnSchema.from_metadata(_variable(name)).hash == name.lower()
    assert SqlColumnSchema.from_data(name, "x").hash == name.lower()


def test_table_with_reserved_word_columns_can_be_created():
    schema = SqlTableSchema("xx", "xx", source="data")
    for name in ["STUDYID", "ORDER", "GROUP", "USER"]:
        schema.add_column(SqlColumnSchema.from_metadata(_variable(name)))

    query = SQLSerialiser.create_table_query_from_schema(schema)

    assert schema.get_column_hash("ORDER") in query
    assert " order " not in query.lower()
