import pytest

from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.get_codelist_attributes import (
    SqlGetCodelistAttributesOperation,
)
from .helpers import assert_operation_collection, assert_operation_parameterized_collection


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


def setup_data_table_with_version_column(data_service: PostgresQLDataService, table_name: str):
    schema = SqlTableSchema.static(table_name)
    schema.add_column(SqlColumnSchema("studydate", "studydate", "Char"))
    data_service.pgi.create_table(schema)
    data_service.pgi.insert_data(table_name, [{"studydate": "2021-12-17"}])


def test_get_codelist_attributes_column_referenced_version_uses_table_not_domain(sdtm_standards_context):
    data_service = PostgresQLDataService.instance()
    setup_codelist_table(data_service)
    setup_data_table_with_version_column(data_service, "ae1")

    params = SqlOperationParams(
        domain="ae",
        table="ae1",
        target="column",
        standards_context=sdtm_standards_context,
        ct_attribute="Term CCODE",
        ct_version="studydate",
    )

    operation = SqlGetCodelistAttributesOperation(params, data_service)
    result = operation.execute()

    assert result.params == {"$ct_version": "studydate"}
    assert_operation_parameterized_collection(
        operation,
        result,
        [{"params": {"$ct_version": "2021-12-17"}, "value": ["C999"]}],
        unsorted=True,
    )


def test_get_codelist_attributes_column_reference_takes_precedence_over_provided_codelists(
    sdtm_standards_context,
):
    data_service = PostgresQLDataService.instance(provided_codelists="sdtmct-2020-03-27")
    setup_codelist_table(data_service)
    setup_data_table_with_version_column(data_service, "ae1")

    params = SqlOperationParams(
        domain="ae",
        table="ae1",
        target="column",
        standards_context=sdtm_standards_context,
        ct_attribute="Term CCODE",
        ct_version="studydate",
    )

    operation = SqlGetCodelistAttributesOperation(params, data_service)
    result = operation.execute()

    assert result.params == {"$ct_version": "studydate"}
    assert_operation_parameterized_collection(
        operation,
        result,
        [
            {"params": {"$ct_version": "2021-12-17"}, "value": ["C999"]},
            {"params": {"$ct_version": " 2021-12-17 "}, "value": ["C999"]},
            {"params": {"$ct_version": None}, "value": ["C1234", "C5678"]},
            {"params": {"$ct_version": ""}, "value": ["C1234", "C5678"]},
            {"params": {"$ct_version": "1999-12-31"}, "value": ["C1234", "C5678"]},
        ],
        unsorted=True,
    )


def test_get_codelist_attributes_without_versions_uses_all_loaded_ct_packages(sdtm_standards_context):
    data_service = PostgresQLDataService.instance()
    setup_codelist_table(data_service)

    params = SqlOperationParams(
        domain="dataset",
        target="column",
        standards_context=sdtm_standards_context,
        ct_attribute="Term CCODE",
    )

    operation = SqlGetCodelistAttributesOperation(params, data_service)
    result = operation.execute()

    assert_operation_collection(operation, result, ["C1234", "C5678", "C999"], unsorted=True)


def test_get_codelist_attributes_empty_column_version(
    sdtm_standards_context,
):
    data_service = PostgresQLDataService.instance()
    setup_codelist_table(data_service)
    setup_data_table_with_version_column(data_service, "ae1")

    params = SqlOperationParams(
        domain="ae",
        table="ae1",
        target="column",
        standards_context=sdtm_standards_context,
        ct_attribute="Term CCODE",
        ct_version="studydate",
    )

    operation = SqlGetCodelistAttributesOperation(params, data_service)
    result = operation.execute()

    assert_operation_parameterized_collection(
        operation,
        result,
        [
            {"params": {"$ct_version": "2020-03-27"}, "value": ["C1234", "C5678"]},
            {"params": {"$ct_version": None}, "value": []},
            {"params": {"$ct_version": ""}, "value": []},
        ],
        unsorted=True,
    )


@pytest.mark.parametrize(
    "provided_codelists, expected",
    [(None, []), ("sdtmct-2021-12-17", ["C999"])],
)
def test_get_codelist_attributes_numeric_column_version_matches_no_ct_package(
    sdtm_standards_context, provided_codelists, expected
):
    data_service = PostgresQLDataService.instance(provided_codelists=provided_codelists)
    setup_codelist_table(data_service)
    schema = SqlTableSchema.static("ae1")
    schema.add_column(SqlColumnSchema("studydate", "studydate", "Num"))
    data_service.pgi.create_table(schema)
    data_service.pgi.insert_data("ae1", [{"studydate": 22001.0}])

    params = SqlOperationParams(
        domain="ae",
        table="ae1",
        target="column",
        standards_context=sdtm_standards_context,
        ct_attribute="Term CCODE",
        ct_version="studydate",
    )

    operation = SqlGetCodelistAttributesOperation(params, data_service)
    result = operation.execute()

    query = result.query.replace("$ct_version", "(SELECT studydate FROM ae1)")
    assert_operation_collection(
        operation, SqlOperationResult(query=query, type=result.type, subtype=result.subtype), expected, unsorted=True
    )


@pytest.mark.parametrize(
    "ct_conditions, expected",
    [
        ([{"Codelist Code": "CL2"}], ["C999"]),
        # the CLI's rule loader replaces spaces in rule keys with underscores
        ([{"Codelist_Code": "CL2"}], ["C999"]),
        ([{"Standard_Type": "sdtm"}, {"Codelist_Name": "Codelist One"}], ["C1234", "C5678"]),
        ([{"Codelist Name": "Codelist O'Brien"}], []),
    ],
)
def test_get_codelist_attributes_ct_conditions(sdtm_standards_context, ct_conditions, expected):
    data_service = PostgresQLDataService.instance()
    setup_codelist_table(data_service)

    params = SqlOperationParams(
        domain="dataset",
        target="column",
        standards_context=sdtm_standards_context,
        ct_attribute="Term CCODE",
        ct_conditions=ct_conditions,
    )

    operation = SqlGetCodelistAttributesOperation(params, data_service)
    result = operation.execute()

    assert_operation_collection(operation, result, expected, unsorted=True)


def test_get_codelist_attributes_unknown_ct_condition_raises_clear_error(sdtm_standards_context):
    data_service = PostgresQLDataService.instance()
    setup_codelist_table(data_service)

    params = SqlOperationParams(
        domain="dataset",
        target="column",
        standards_context=sdtm_standards_context,
        ct_attribute="Term CCODE",
        ct_conditions=[{"Not An Attribute": "x"}],
    )

    with pytest.raises(Exception, match="Unknown ct_conditions attribute 'Not An Attribute'"):
        SqlGetCodelistAttributesOperation(params, data_service).execute()
