from cdisc_rules_engine.data_service.merges.join import SqlJoinMerge
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
    assert_operation_collection,
)


def test_dataset_names():
    data_service = PostgresQLDataService.instance()
    t1 = PostgresQLDataService.add_test_dataset(
        data_service, "t1", {"key": [1, 2, 3], "name": ["A", "B", "C"]}, DefaultStandardsContext()
    )

    t2 = PostgresQLDataService.add_test_dataset(
        data_service, "t2", {"key": [1, 2, 3], "age": [10, 20, 30]}, DefaultStandardsContext()
    )

    SqlJoinMerge.perform_join(data_service.pgi, t1, t2, ["key"], ["key"])

    params = SqlOperationParams(domain="t1", target=None, standards_context=None)
    operation = SqlOperationsFactory.get_service("dataset_names", params, data_service)
    result = operation.execute()
    assert_operation_collection(operation, result, ["t1", "t2"], unsorted=True)
