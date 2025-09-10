import pandas as pd
import pytest
from numpy import NaN

from cdisc_rules_engine.data_service.merges.join import SqlJoinMerge
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)

SIMPLE_DATA = {
    "left": {"key": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]},
    "right": {"key": [1, 2, 4], "age": [30, 25, 40]},
}


@pytest.mark.parametrize(
    "data",
    [SIMPLE_DATA],
)
@pytest.mark.parametrize(
    "type, result",
    [
        (
            "INNER",
            [
                {"key": 1, "name": "Alice", "age": 30},
                {"key": 2, "name": "Bob", "age": 25},
            ],
        ),
        (
            "LEFT",
            [
                {"key": 1, "name": "Alice", "age": 30},
                {"key": 2, "name": "Bob", "age": 25},
                {"key": 3, "name": "Charlie", "age": NaN},
            ],
        ),
        (
            "RIGHT",
            [
                {"key": 1, "name": "Alice", "age": 30},
                {"key": 2, "name": "Bob", "age": 25},
                {"key": 4, "name": None, "age": 40},
            ],
        ),
        (
            "FULL OUTER",
            [
                {"key": 1, "name": "Alice", "age": 30},
                {"key": 2, "name": "Bob", "age": 25},
                {"key": 3, "name": "Charlie", "age": NaN},
                {"key": 4, "name": None, "age": 40},
            ],
        ),
    ],
)
def test_join(data, type, result):
    ds = PostgresQLDataService.test_instance()
    PostgresQLDataService.add_test_dataset(ds.pgi, "l", data["left"])
    PostgresQLDataService.add_test_dataset(ds.pgi, "r", data["right"])

    # Perform the join operation
    schema = SqlJoinMerge.perform_join(
        ds.pgi, ds.pgi.schema.get_table("l"), ds.pgi.schema.get_table("r"), ["key"], ["key"], type
    )

    assert schema.get_column("key") is not None
    assert schema.get_column("name") is not None
    assert schema.get_column("age") is not None
    assert ds.pgi.schema.get_table(schema.name) == schema

    ds.pgi.execute_sql(f"SELECT key, name, {schema.get_column_hash("age")} AS age FROM {schema.hash}")
    result = pd.DataFrame(ds.pgi.fetch_all())
    expected_result = pd.DataFrame(result)
    assert expected_result.equals(result)


@pytest.mark.parametrize(
    "data, result",
    [
        (
            {
                "left": {
                    "key": [1, 1, 2, 2, 3],
                    "key2": [1, 2, 1, 2, 1],
                    "name": ["Alice A", "Alice B", "Bob A", "Bob B", "Charlie"],
                },
                "right": {
                    "key1": [1, 1, 2, 3, 4],
                    "key2": [1, 2, 2, 1, 1],
                    "age": [10, 20, 30, 40, 50],
                    "name": ["AA", "AB", "BB", "C", "D"],
                },
            },
            [
                {"key": 1, "key2": 1, "name": "Alice A", "age": 10},
                {"key": 1, "key2": 2, "name": "Alice B", "age": 20},
                {"key": 2, "key2": 2, "name": "Bob B", "age": 30},
                {"key": 3, "key2": 1, "name": "Charlie", "age": 40},
            ],
        ),
    ],
)
def test_multiple_keys(data, result):
    ds = PostgresQLDataService.test_instance()
    PostgresQLDataService.add_test_dataset(ds.pgi, "l", data["left"])
    PostgresQLDataService.add_test_dataset(ds.pgi, "r", data["right"])

    # Perform the join operation
    schema = SqlJoinMerge.perform_join(
        ds.pgi, ds.pgi.schema.get_table("l"), ds.pgi.schema.get_table("r"), ["key", "key2"], ["key1", "key2"]
    )

    assert schema.get_column("key") is not None
    assert schema.get_column("name") is not None
    assert schema.get_column("age") is not None
    assert ds.pgi.schema.get_table(schema.name) == schema

    ds.pgi.execute_sql(f"SELECT key, name, {schema.get_column_hash("age")} AS age FROM {schema.hash}")
    result = pd.DataFrame(ds.pgi.fetch_all())
    expected_result = pd.DataFrame(result)
    assert expected_result.equals(result)


@pytest.mark.parametrize(
    "data",
    [SIMPLE_DATA],
)
def test_table_not_in_data(data):
    ds = PostgresQLDataService.test_instance()
    PostgresQLDataService.add_test_dataset(ds.pgi, "l", data["left"])
    PostgresQLDataService.add_test_dataset(ds.pgi, "r", data["right"])

    # Perform the join operation
    with pytest.raises(Exception):
        SqlJoinMerge.perform_join(
            ds.pgi, ds.pgi.schema.get_table("l"), ds.pgi.schema.get_table("unknown"), ["key", "key2"], ["key1", "key2"]
        )


@pytest.mark.parametrize(
    "data",
    [SIMPLE_DATA],
)
def test_column_not_in_data(data):
    ds = PostgresQLDataService.test_instance()
    PostgresQLDataService.add_test_dataset(ds.pgi, "l", data["left"])
    PostgresQLDataService.add_test_dataset(ds.pgi, "r", data["right"])

    # Perform the join operation
    with pytest.raises(Exception):
        SqlJoinMerge.perform_join(
            ds.pgi, ds.pgi.schema.get_table("l"), ds.pgi.schema.get_table("r"), ["key", "key2"], ["key1", "key2"]
        )


@pytest.mark.parametrize(
    "data",
    [SIMPLE_DATA],
)
def test_wrong_column_number(data):
    ds = PostgresQLDataService.test_instance()
    PostgresQLDataService.add_test_dataset(ds.pgi, "l", data["left"])
    PostgresQLDataService.add_test_dataset(ds.pgi, "r", data["right"])

    # Perform the join operation
    with pytest.raises(Exception):
        SqlJoinMerge.perform_join(
            ds.pgi, ds.pgi.schema.get_table("l"), ds.pgi.schema.get_table("r"), ["key", "key2"], ["key1"]
        )


@pytest.mark.parametrize(
    "data",
    [SIMPLE_DATA],
)
def test_run_twice(data):
    ds = PostgresQLDataService.test_instance()
    PostgresQLDataService.add_test_dataset(ds.pgi, "l", data["left"])
    PostgresQLDataService.add_test_dataset(ds.pgi, "r", data["right"])

    # Perform the join operation
    schema = SqlJoinMerge.perform_join(
        ds.pgi, ds.pgi.schema.get_table("l"), ds.pgi.schema.get_table("r"), ["key"], ["key"]
    )

    schema2 = SqlJoinMerge.perform_join(
        ds.pgi, ds.pgi.schema.get_table("l"), ds.pgi.schema.get_table("r"), ["key"], ["key"]
    )

    assert schema == schema2


@pytest.mark.parametrize(
    "data",
    [SIMPLE_DATA],
)
def test_join_table_itself(data):
    ds = PostgresQLDataService.test_instance()
    PostgresQLDataService.add_test_dataset(ds.pgi, "l", data["left"])

    with pytest.raises(Exception):
        SqlJoinMerge.perform_join(ds.pgi, ds.pgi.schema.get_table("l"), ds.pgi.schema.get_table("l"), ["key"], ["key"])
