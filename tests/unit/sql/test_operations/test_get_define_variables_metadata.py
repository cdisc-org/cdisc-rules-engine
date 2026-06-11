from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.get_define_variables_metadata import (
    SqlGetDefineVariablesMetadata,
)
from .helpers import assert_operation_constant

define_path = "tests/resources/define.xml"


def test_get_define_variable_metadata_collected(sdtm_standards_context):
    data_service = PostgresQLDataService.instance(define_xml_path=define_path)
    params = SqlOperationParams(
        domain="AE",
        target="AEENTPT",
        standards_context=sdtm_standards_context,
        attribute_name="define_variable_is_collected",
    )
    operation = SqlGetDefineVariablesMetadata(params=params, data_service=data_service)
    result = operation.execute()

    assert_operation_constant(operation, result, expected="False")


def test_get_define_variable_metadata_label(sdtm_standards_context):
    data_service = PostgresQLDataService.instance(define_xml_path=define_path)
    params = SqlOperationParams(
        domain="AE",
        target="AEENTPT",
        standards_context=sdtm_standards_context,
        attribute_name="define_variable_label",
    )
    operation = SqlGetDefineVariablesMetadata(params=params, data_service=data_service)
    result = operation.execute()

    assert_operation_constant(operation, result, expected="End Reference Time Point")
