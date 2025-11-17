from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)
from cdisc_rules_engine.standards.default_standards_context import (
    DefaultStandardsContext,
)

from .helpers import (
    assert_operation_constant,
)


class DummyStandardsContext(DefaultStandardsContext):
    def get_standard_metadata(self):
        return {"domains": {"AE", "SUPPQUAL"}}


def test_domain_is_custom_false():
    data_service = PostgresQLDataService.instance()
    params = SqlOperationParams(domain="AE", target=None, standards_context=DummyStandardsContext())
    operation = SqlOperationsFactory.get_service("domain_is_custom", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, False)


def test_domain_is_custom_true():
    data_service = PostgresQLDataService.instance()
    params = SqlOperationParams(domain="ZZ", target=None, standards_context=DummyStandardsContext())
    operation = SqlOperationsFactory.get_service("domain_is_custom", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, True)
