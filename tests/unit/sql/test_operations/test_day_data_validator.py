import pytest

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)

from .helpers import assert_operation_constant, assert_operation_parameterized_constant


@pytest.mark.parametrize(
    "current_data, dm_data, expected",
    [
        (
            {
                "USUBJID": [1, 2, 3, 4, 5, 6, 7],
                "EXSTDTC": [
                    "1997-07-19T19:20:30",
                    "1997-08-16T19:20:30",
                    "1997-07-16T19:20",
                    "2022-05-20T13:44",
                    "2022-05-20T13:44",
                    None,
                    "2022-05-19T13:44",
                ],
            },
            {
                "USUBJID": [1, 2, 3, 4, 5, 6, 7],
                "RFSTDTC": [
                    "1997-07-16T19:20:30",
                    "1997-07-16T19:20:30",
                    "1997-07-16T19:20",
                    "2022-05-08T13:44",
                    "TEST",
                    "2022-05-20T13:44",
                    "2022-05-20T13:44",
                ],
            },
            [
                {"params": {"$1": 1}, "value": [4]},
                {"params": {"$1": 2}, "value": [32]},
                {"params": {"$1": 3}, "value": [1]},
                {"params": {"$1": 4}, "value": [13]},
                {"params": {"$1": 5}, "value": [None]},
                {"params": {"$1": 6}, "value": [None]},
                {"params": {"$1": 7}, "value": [-1]},
            ],
        ),
        (
            {
                "USUBJID": [1, 2, 3],
                "EXSTDTC": [
                    "2023-01-01T12:00:00",
                    "2023-01-02T00:00:00",
                    "2022-12-31T23:59:59",
                ],
            },
            {
                "USUBJID": [1, 2, 3],
                "RFSTDTC": [
                    "2023-01-01T00:00:00",
                    "2023-01-01T12:00:00",
                    "2023-01-01T00:00:00",
                ],
            },
            [
                {"params": {"$1": 1}, "value": [1]},
                {"params": {"$1": 2}, "value": [2]},
                {"params": {"$1": 3}, "value": [-1]},
            ],
        ),
    ],
)
def test_sql_dy_calculation(current_data, dm_data, expected, sdtm_standards_context):
    data_service = PostgresQLDataService.instance()

    PostgresQLDataService.add_test_dataset(
        data_service, table_name="DM", column_data=dm_data, standards_context=sdtm_standards_context
    )
    PostgresQLDataService.add_test_dataset(
        data_service, table_name="EX", column_data=current_data, standards_context=sdtm_standards_context
    )

    params = SqlOperationParams(domain="EX", target="EXSTDTC", standards_context=sdtm_standards_context)
    operation = SqlOperationsFactory.get_service("dy", params, data_service)
    result = operation.execute()

    assert_operation_parameterized_constant(operation, result, expected)


@pytest.mark.parametrize(
    "current_data, expected",
    [
        (
            {
                "USUBJID": [1, 2, 3],
                "EXSTDTC": [
                    "2023-01-01T12:00:00",
                    "2023-01-02T00:00:00",
                    "2022-12-31T23:59:59",
                ],
            },
            0,
        ),
    ],
)
def test_sql_dy_no_dm_domain(current_data, expected, sdtm_standards_context):
    data_service = PostgresQLDataService.instance()

    PostgresQLDataService.add_test_dataset(
        data_service, table_name="EX", column_data=current_data, standards_context=sdtm_standards_context
    )

    params = SqlOperationParams(domain="EX", target="EXSTDTC", standards_context=sdtm_standards_context)
    operation = SqlOperationsFactory.get_service("dy", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize(
    "current_data, dm_data, expected",
    [
        (
            {
                "USUBJID": [1, 2, 999],
                "EXSTDTC": [
                    "1997-07-19T19:20:30",
                    "1997-08-16T19:20:30",
                    "1997-07-16T19:20",
                ],
            },
            {
                "USUBJID": [1, 2],
                "RFSTDTC": [
                    "1997-07-16T19:20:30",
                    "1997-07-16T19:20:30",
                ],
            },
            [
                {"params": {"$1": 1}, "value": [4]},
                {"params": {"$1": 2}, "value": [32]},
                {"params": {"$1": 3}, "value": [None]},
            ],
        ),
    ],
)
def test_sql_dy_missing_usubjid(current_data, dm_data, expected, sdtm_standards_context):
    data_service = PostgresQLDataService.instance()

    PostgresQLDataService.add_test_dataset(
        data_service, table_name="DM", column_data=dm_data, standards_context=sdtm_standards_context
    )
    PostgresQLDataService.add_test_dataset(
        data_service, table_name="EX", column_data=current_data, standards_context=sdtm_standards_context
    )

    params = SqlOperationParams(domain="EX", target="EXSTDTC", standards_context=sdtm_standards_context)
    operation = SqlOperationsFactory.get_service("dy", params, data_service)
    result = operation.execute()

    assert_operation_parameterized_constant(operation, result, expected)


@pytest.mark.parametrize(
    "current_data, dm_data, expected",
    [
        (
            {
                "USUBJID": [1, 2, 3, 4],
                "EXSTDTC": [
                    "invalid-date",
                    "2023-1-1",
                    "",
                    "2023-01-01",
                ],
            },
            {
                "USUBJID": [1, 2, 3, 4],
                "RFSTDTC": [
                    "2023-01-01T00:00:00",
                    "2023-01-01T00:00:00",
                    "2023-01-01T00:00:00",
                    "2023-01-01T00:00:00",
                ],
            },
            [
                {"params": {"$1": 1}, "value": [None]},
                {"params": {"$1": 2}, "value": [None]},
                {"params": {"$1": 3}, "value": [None]},
                {"params": {"$1": 4}, "value": [1]},
            ],
        ),
    ],
)
def test_sql_dy_invalid_dates(current_data, dm_data, expected, sdtm_standards_context):
    """Test DY calculation with various invalid date formats."""
    data_service = PostgresQLDataService.instance()

    PostgresQLDataService.add_test_dataset(
        data_service, table_name="DM", column_data=dm_data, standards_context=sdtm_standards_context
    )
    PostgresQLDataService.add_test_dataset(
        data_service, table_name="EX", column_data=current_data, standards_context=sdtm_standards_context
    )

    params = SqlOperationParams(domain="EX", target="EXSTDTC", standards_context=sdtm_standards_context)
    operation = SqlOperationsFactory.get_service("dy", params, data_service)
    result = operation.execute()

    assert_operation_parameterized_constant(operation, result, expected)
