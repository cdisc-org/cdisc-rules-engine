import pytest

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)
from .helpers import assert_operation_table


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
                {"id": 1, "value": 4},
                {"id": 2, "value": 32},
                {"id": 3, "value": 1},
                {"id": 4, "value": 13},
                {"id": 5, "value": None},
                {"id": 6, "value": None},
                {"id": 7, "value": -1},
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
                {"id": 1, "value": 1},
                {"id": 2, "value": 2},
                {"id": 3, "value": -1},
            ],
        ),
    ],
)
def test_sql_dy_calculation(current_data, dm_data, expected):
    data_service = PostgresQLDataService.test_instance()

    PostgresQLDataService.add_test_dataset(data_service.pgi, table_name="DM", column_data=dm_data)
    PostgresQLDataService.add_test_dataset(data_service.pgi, table_name="EX", column_data=current_data)

    params = SqlOperationParams(domain="EX", target="EXSTDTC", standard="", standard_version="")
    operation = SqlOperationsFactory.get_service("dy", params, data_service)
    result = operation.execute()

    assert_operation_table(operation, result, expected)


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
            [
                {"id": 1, "value": 0},
                {"id": 2, "value": 0},
                {"id": 3, "value": 0},
            ],
        ),
    ],
)
def test_sql_dy_no_dm_domain(current_data, expected):
    data_service = PostgresQLDataService.test_instance()

    PostgresQLDataService.add_test_dataset(data_service.pgi, table_name="EX", column_data=current_data)

    params = SqlOperationParams(domain="EX", target="EXSTDTC", standard="", standard_version="")
    operation = SqlOperationsFactory.get_service("dy", params, data_service)
    result = operation.execute()

    assert_operation_table(operation, result, expected)


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
                {"id": 1, "value": 4},
                {"id": 2, "value": 32},
                {"id": 3, "value": None},
            ],
        ),
    ],
)
def test_sql_dy_missing_usubjid(current_data, dm_data, expected):
    data_service = PostgresQLDataService.test_instance()

    PostgresQLDataService.add_test_dataset(data_service.pgi, table_name="DM", column_data=dm_data)
    PostgresQLDataService.add_test_dataset(data_service.pgi, table_name="EX", column_data=current_data)

    params = SqlOperationParams(domain="EX", target="EXSTDTC", standard="", standard_version="")
    operation = SqlOperationsFactory.get_service("dy", params, data_service)
    result = operation.execute()

    assert_operation_table(operation, result, expected)


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
                {"id": 1, "value": None},
                {"id": 2, "value": None},
                {"id": 3, "value": None},
                {"id": 4, "value": 1},
            ],
        ),
    ],
)
def test_sql_dy_invalid_dates(current_data, dm_data, expected):
    """Test DY calculation with various invalid date formats."""
    data_service = PostgresQLDataService.test_instance()

    PostgresQLDataService.add_test_dataset(data_service.pgi, table_name="DM", column_data=dm_data)
    PostgresQLDataService.add_test_dataset(data_service.pgi, table_name="EX", column_data=current_data)

    params = SqlOperationParams(domain="EX", target="EXSTDTC", standard="", standard_version="")
    operation = SqlOperationsFactory.get_service("dy", params, data_service)
    result = operation.execute()

    assert_operation_table(operation, result, expected)
