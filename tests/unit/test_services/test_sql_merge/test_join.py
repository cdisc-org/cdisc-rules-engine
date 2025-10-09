import pandas as pd
import pytest

from cdisc_rules_engine.constants.metadata_columns import SOURCE_ROW_NUMBER
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


def test_source_row_number_preserved_through_join():
    ds = PostgresQLDataService.instance()
    sv_data = {
        "STUDYID": ["STUDY1"] * 20,
        "USUBJID": ["1201001"] * 20,
        "SVSTDTC": [
            "2018-08-13",
            "2018-08-14",
            "2018-08-15",
            "2018-08-16",
            "2018-08-17",
            "2018-08-18",
            "2018-08-19",
            "2018-08-20",
            "2018-08-21",
            "2018-08-22",
            "2018-08-23",
            "2018-08-24",
            "2018-08-25",
            "2018-08-26",
            "2018-08-27",
            "2018-08-28",
            "2018-08-29",
            "2018-08-30",
            "2018-08-31",
            "2018-09-01",
        ],
        "EPOCH": ["SCREENING"] * 20,
    }
    se_data = {
        "STUDYID": ["STUDY1"],
        "USUBJID": ["1201001"],
        "SESTDTC": ["2018-08-13"],
        "SEENDTC": ["2018-08-19"],
        "EPOCH": ["TREATMENT"],
    }
    sv_schema = PostgresQLDataService.add_test_dataset(ds, "sv", sv_data)
    se_schema = PostgresQLDataService.add_test_dataset(ds, "se", se_data)

    result = SqlJoinMerge.perform_join(
        pgi=ds.pgi, left=sv_schema, right=se_schema, pivot_left=["USUBJID"], pivot_right=["USUBJID"], type="LEFT"
    )

    ds.pgi.execute_sql(
        f"""
            SELECT {SOURCE_ROW_NUMBER}, epoch, {result.get_column_hash('se.epoch')} as se_epoch
            FROM {result.hash}
            WHERE epoch != {result.get_column_hash('se.epoch')}
            ORDER BY {SOURCE_ROW_NUMBER}
        """
    )
    errors = ds.pgi.fetch_all()
    assert len(errors) > 0
    first_error_row = errors[0][SOURCE_ROW_NUMBER]
    assert first_error_row == 1, f"Expected row 1, got row {first_error_row}"
