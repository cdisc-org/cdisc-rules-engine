from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.standards.default_standards_context import DefaultStandardsContext
from cdisc_rules_engine.sql_operations.variable_count import SqlVariableCountOperation


def test_variable_count_single_domain():
    """Test when variable exists in one domain only"""
    data_service = PostgresQLDataService.instance()

    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="AE",
        column_data={"AELNKGRP": ["A", "B"], "AESEQ": [1, 2]},
        standards_context=DefaultStandardsContext(),
    )
    PostgresQLDataService.add_test_dataset(
        data_service, table_name="VS", column_data={"VSSEQ": [1, 2]}, standards_context=DefaultStandardsContext()
    )

    params = SqlOperationParams(domain="AE", target="AELNKGRP", standards_context=None)

    operation = SqlVariableCountOperation(params, data_service)
    result = operation.execute()

    data_service.pgi.execute_sql(result.query)
    rows = data_service.pgi.fetch_all()

    assert len(rows) == 1
    assert rows[0]["value"] == 1


def test_variable_count_multiple_domains():
    """Test when variable exists in multiple domains"""
    data_service = PostgresQLDataService.instance()

    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="AE",
        column_data={"AELNKGRP": ["A", "B"], "AESEQ": [1, 2]},
        standards_context=DefaultStandardsContext(),
    )
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="FA",
        column_data={"FALNKGRP": ["C", "D"], "FASEQ": [1, 2]},
        standards_context=DefaultStandardsContext(),
    )
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="VS",
        column_data={"VSLNKGRP": ["E", "F"], "VSSEQ": [1, 2]},
        standards_context=DefaultStandardsContext(),
    )

    params = SqlOperationParams(domain="AE", target="AELNKGRP", standards_context=None)

    operation = SqlVariableCountOperation(params, data_service)
    result = operation.execute()

    data_service.pgi.execute_sql(result.query)
    rows = data_service.pgi.fetch_all()

    assert len(rows) == 1
    assert rows[0]["value"] == 3

    params.target = "FALNKGRP"
    params.domain = "FA"

    operation = SqlVariableCountOperation(params, data_service)
    result = operation.execute()

    data_service.pgi.execute_sql(result.query)
    rows = data_service.pgi.fetch_all()

    assert len(rows) == 1
    assert rows[0]["value"] == 3


def test_variable_count_not_found():
    """Test when variable doesn't exist in any domain"""
    data_service = PostgresQLDataService.instance()

    PostgresQLDataService.add_test_dataset(
        data_service, table_name="AE", column_data={"AESEQ": [1, 2]}, standards_context=DefaultStandardsContext()
    )
    PostgresQLDataService.add_test_dataset(
        data_service, table_name="VS", column_data={"VSSEQ": [1, 2]}, standards_context=DefaultStandardsContext()
    )

    params = SqlOperationParams(domain="AE", target="NONEXISTENT", standards_context=None)

    operation = SqlVariableCountOperation(params, data_service)
    result = operation.execute()

    data_service.pgi.execute_sql(result.query)
    rows = data_service.pgi.fetch_all()

    assert len(rows) == 1
    assert rows[0]["value"] == 0
