import pytest

from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.define_exdict_version_operation import (
    SqlDefineExternalDictionaryVersionOperation,
)
from .helpers import assert_operation_constant

define_path = "tests/resources/define.xml"


@pytest.mark.parametrize("dictionary, version", [("meddra", "22.0"), ("snomed", "2019-09-01")])
def test_get_define_exdict_version(sdtm_standards_context, dictionary, version):
    data_service = PostgresQLDataService.instance(define_xml_path=define_path)
    params = SqlOperationParams(
        domain="AE",
        target="FAKEVARIABLE",
        standards_context=sdtm_standards_context,
        external_dictionary_type=dictionary,
    )
    operation = SqlDefineExternalDictionaryVersionOperation(params=params, data_service=data_service)
    result = operation.execute()

    assert_operation_constant(operation, result, expected=version)
