from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.get_codelist_attributes import (
    SqlGetCodelistAttributesOperation,
)
from .helpers import assert_operation_collection


def setup_codelist_table(data_service: PostgresQLDataService):
    table_name = StaticTables.IG_CODELIST_TABLE_NAME.value
    schema = SqlTableSchema.static(table_name)
    schema.add_column(SqlColumnSchema("standard_type", "standard_type", "Char"))
    schema.add_column(SqlColumnSchema("version_date", "version_date", "Char"))
    schema.add_column(SqlColumnSchema("item_code", "item_code", "Char"))
    schema.add_column(SqlColumnSchema("value", "value", "Char"))
    schema.add_column(SqlColumnSchema("codelist_code", "codelist_code", "Char"))
    schema.add_column(SqlColumnSchema("name", "name", "Char"))
    schema.add_column(SqlColumnSchema("term", "term", "Char"))

    data_service.pgi.create_table(schema)

    data = [
        {
            "standard_type": "sdtm",
            "version_date": "2020-03-27",
            "item_code": "C1234",
            "value": "Signification A",
            "codelist_code": "CL1",
            "name": "Codelist One",
            "term": "Term A",
        },
        {
            "standard_type": "sdtm",
            "version_date": "2020-03-27",
            "item_code": "C5678",
            "value": "Signification B",
            "codelist_code": "CL1",
            "name": "Codelist One",
            "term": "Term B",
        },
        {
            "standard_type": "sdtm",
            "version_date": "2021-12-17",
            "item_code": "C999",
            "value": "Signification C",
            "codelist_code": "CL2",
            "name": "Codelist Two",
            "term": "Term C",
        },
        {
            "standard_type": "sdtm",
            "version_date": "2021-12-17",
            "item_code": "C999",
            "value": "Signification D",
            "codelist_code": "CL2",
            "name": "Codelist Two",
            "term": "Term D",
        },
    ]
    data_service.pgi.insert_data(table_name, data)


def test_get_codelist_attributes_term_ccode(sdtm_standards_context):
    data_service = PostgresQLDataService.instance(provided_codelists="sdtmct-2020-03-27")
    setup_codelist_table(data_service)

    params = SqlOperationParams(
        domain="dataset",
        target="column",
        standards_context=sdtm_standards_context,
        ct_attribute="Term CCODE",
    )

    operation = SqlGetCodelistAttributesOperation(params, data_service)
    result = operation.execute()

    assert_operation_collection(operation, result, ["C1234", "C5678"], unsorted=True)


def test_get_codelist_attributes_term_signification(sdtm_standards_context):
    data_service = PostgresQLDataService.instance(provided_codelists="sdtmct-2021-12-17")
    setup_codelist_table(data_service)

    params = SqlOperationParams(
        domain="dataset",
        target="column",
        standards_context=sdtm_standards_context,
        ct_attribute="Term Signification",
    )

    operation = SqlGetCodelistAttributesOperation(params, data_service)
    result = operation.execute()

    assert_operation_collection(operation, result, ["Signification C", "Signification D"], unsorted=True)
