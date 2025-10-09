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
    def get_domain_label(self, domain):
        if domain == "AE":
            return "Adverse Events"
        else:
            return ""


def test_domain_label():
    data_service = PostgresQLDataService.instance()
    params = SqlOperationParams(domain="AE", target=None, standards_context=DummyStandardsContext())
    operation = SqlOperationsFactory.get_service("domain_label", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, "Adverse Events")


def test_domain_label_custom_domain():
    data_service = PostgresQLDataService.instance()
    params = SqlOperationParams(domain="AA", target=None, standards_context=DummyStandardsContext())
    operation = SqlOperationsFactory.get_service("domain_label", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, "")  # Empty label when domain not found
