from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)
from cdisc_rules_engine.standards.default_standards_context import DefaultStandardsContext

from .helpers import (
    assert_operation_collection,
)


class DummyStandardsContext(DefaultStandardsContext):
    def get_standard_metadata(self):
        return {
            "domains": ["DM", "AE", "LB"],
        }


def test_standard_domains_operation():
    data_service = PostgresQLDataService.instance()
    standards_context = DummyStandardsContext()
    params = SqlOperationParams(domain="DUMMY", target="DUMMY", standards_context=standards_context)
    operation = SqlOperationsFactory.get_service("standard_domains", params, data_service)
    result = operation.execute()
    assert_operation_collection(operation, result, ["AE", "DM", "LB"], unsorted=False)
