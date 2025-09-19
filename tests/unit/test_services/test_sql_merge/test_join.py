import pandas as pd
import pytest

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
    "type, expected",
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
                {"key": 3, "name": "Charlie", "age": None},
            ],
        ),
        (
            "RIGHT",
            [
                {"key": 1, "name": "Alice", "age": 30},
                {"key": 2, "name": "Bob", "age": 25},
                {"key": None, "name": None, "age": 40},
            ],
        ),
        (
            "FULL OUTER",
            [
                {"key": 1, "name": "Alice", "age": 30},
                {"key": 2, "name": "Bob", "age": 25},
                {"key": 3, "name": "Charlie", "age": None},
                {"key": None, "name": None, "age": 40},
            ],
        ),
    ],
)
def test_join(data, type, expected):
    ds = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(ds, "l", data["left"])
    PostgresQLDataService.add_test_dataset(ds, "r", data["right"])

    # Perform the join operation
    schema = SqlJoinMerge.perform_join(
        ds.pgi, ds.pgi.schema.get_table("l"), ds.pgi.schema.get_table("r"), ["key"], ["key"], type
    )

    assert schema.get_column("key") is not None
    assert schema.get_column("name") is not None
    assert schema.get_column("age") is not None
    assert schema.get_column("r.age") is not None
    assert ds.pgi.schema.get_table(schema.name) == schema

    ds.pgi.execute_sql(f"SELECT key::int, name, {schema.get_column_hash("age")}::int AS age FROM {schema.hash}")
    result = pd.DataFrame(ds.pgi.fetch_all())
    assert pd.DataFrame(expected).equals(result)


@pytest.mark.parametrize(
    "data, expected",
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
                {"key": 1, "key2": 1, "name": "Alice A", "age": 10, "r.name": "AA"},
                {"key": 1, "key2": 2, "name": "Alice B", "age": 20, "r.name": "AB"},
                {"key": 2, "key2": 2, "name": "Bob B", "age": 30, "r.name": "BB"},
                {"key": 3, "key2": 1, "name": "Charlie", "age": 40, "r.name": "C"},
            ],
        ),
    ],
)
def test_multiple_keys(data, expected):
    ds = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(ds, "l", data["left"])
    PostgresQLDataService.add_test_dataset(ds, "r", data["right"])

    # Perform the join operation
    schema = SqlJoinMerge.perform_join(
        ds.pgi, ds.pgi.schema.get_table("l"), ds.pgi.schema.get_table("r"), ["key", "key2"], ["key1", "key2"]
    )

    assert schema.get_column("key") is not None
    assert schema.get_column("name") is not None
    assert schema.get_column("age") is not None
    assert schema.get_column("r.age") is not None
    assert schema.get_column("r.name") is not None
    assert ds.pgi.schema.get_table(schema.name) == schema

    ds.pgi.execute_sql(
        f"""SELECT
                key::int,
                key2::int,
                name,
                {schema.get_column_hash("age")}::int AS age,
                {schema.get_column_hash("r.name")} as "r.name"
            FROM {schema.hash}"""
    )
    result = pd.DataFrame(ds.pgi.fetch_all())
    assert pd.DataFrame(expected).equals(result)


@pytest.mark.parametrize(
    "data",
    [SIMPLE_DATA],
)
def test_table_not_in_data(data):
    ds = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(ds, "l", data["left"])
    PostgresQLDataService.add_test_dataset(ds, "r", data["right"])

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
    ds = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(ds, "l", data["left"])
    PostgresQLDataService.add_test_dataset(ds, "r", data["right"])

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
    ds = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(ds, "l", data["left"])
    PostgresQLDataService.add_test_dataset(ds, "r", data["right"])

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
    ds = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(ds, "l", data["left"])
    PostgresQLDataService.add_test_dataset(ds, "r", data["right"])

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
    ds = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(ds, "l", data["left"])

    schema = SqlJoinMerge.perform_join(
        ds.pgi, ds.pgi.schema.get_table("l"), ds.pgi.schema.get_table("l"), ["key"], ["key"]
    )
    assert schema.has_column("key")
    assert schema.has_column("name")
    assert schema.has_column("l.name")
