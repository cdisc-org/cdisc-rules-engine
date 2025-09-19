from cdisc_rules_engine.data_service.merges.join import SqlJoinMerge
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)

from .helpers import (
    assert_operation_list,
)


def test_dataset_names():
    data_service = PostgresQLDataService.instance()
    t1 = PostgresQLDataService.add_test_dataset(
        data_service, table_name="t1", column_data={"key": [1, 2, 3], "name": ["A", "B", "C"]}
    )

    t2 = PostgresQLDataService.add_test_dataset(
        data_service, table_name="t2", column_data={"key": [1, 2, 3], "age": [10, 20, 30]}
    )

    SqlJoinMerge.perform_join(data_service.pgi, t1, t2, ["key"], ["key"])

    params = SqlOperationParams(domain="t1", target=None, standard="", standard_version="")
    operation = SqlOperationsFactory.get_service("dataset_names", params, data_service)
    result = operation.execute()
    assert_operation_list(operation, result, ["t1", "t2"], unsorted=True)
