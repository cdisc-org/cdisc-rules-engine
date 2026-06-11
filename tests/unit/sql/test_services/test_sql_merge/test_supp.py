import pandas as pd
import pytest

from cdisc_rules_engine.data_service.merges.supp import SqlSuppMerge
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema

SIMPLE_DATA_DM = {
    "original": {
        "STUDYID": ["A", "A", "A"],
        "DOMAIN": ["DM", "DM", "DM"],
        "USUBJID": ["A", "B", "C"],
        "SEQ": [1, 1, 1],
    },
    "supp": {
        "STUDYID": ["A", "A", "A"],
        "RDOMAIN": ["DM", "DM", "DM"],
        "USUBJID": ["A", "B", "C"],
        "QNAM": ["COLA", "COLA", "COLB"],
        "QVAL": ["VAL1", "VAL2", "VAL3"],
    },
}
SIMPLE_DATA_AE = {
    "original": {
        "STUDYID": ["A", "A", "A"],
        "DOMAIN": ["AE", "AE", "AE"],
        "USUBJID": ["A", "B", "C"],
        "SEQ": [1, 1, 1],
    },
    "supp": {
        "STUDYID": ["A", "A", "A"],
        "RDOMAIN": ["AE", "AE", "AE"],
        "USUBJID": ["A", "B", "C"],
        "IDVAR": ["SEQ", "SEQ", "SEQ"],
        "IDVARVAL": ["1", "1", "1"],
        "QNAM": ["COLA", "COLA", "COLB"],
        "QVAL": ["VAL1", "VAL2", "VAL3"],
    },
}
COMPLEX_DATA_AE = {
    "original": {
        "STUDYID": ["A", "A", "A", "A"],
        "DOMAIN": ["AE", "AE", "AE", "AE"],
        "USUBJID": ["A", "A", "B", "C"],
        "SEQ": [1, 2, 1, 1],
        "ANOTHER": [10, 20, 30, 40],
    },
    "supp": {
        "STUDYID": ["A", "A", "A", "A"],
        "RDOMAIN": ["AE", "AE", "AE", "AE"],
        "USUBJID": ["A", "A", "C", "A"],
        "IDVAR": ["SEQ", "SEQ", "ANOTHER", "ANOTHER"],
        "IDVARVAL": ["1", "2", "40", "10"],
        "QNAM": ["COLA", "COLA", "COLA", "COLB"],
        "QVAL": ["VAL1", "VAL1", "VAL1", "VAL2"],
    },
}


@pytest.mark.parametrize(
    "data, expected, domain",
    [
        (
            SIMPLE_DATA_DM,
            [
                {"studyid": "A", "domain": "DM", "usubjid": "A", "seq": 1, "cola": "VAL1", "colb": None},
                {"studyid": "A", "domain": "DM", "usubjid": "B", "seq": 1, "cola": "VAL2", "colb": None},
                {"studyid": "A", "domain": "DM", "usubjid": "C", "seq": 1, "cola": None, "colb": "VAL3"},
            ],
            "DM",
        ),
        (
            SIMPLE_DATA_AE,
            [
                {"studyid": "A", "domain": "AE", "usubjid": "A", "seq": 1, "cola": "VAL1", "colb": None},
                {"studyid": "A", "domain": "AE", "usubjid": "B", "seq": 1, "cola": "VAL2", "colb": None},
                {"studyid": "A", "domain": "AE", "usubjid": "C", "seq": 1, "cola": None, "colb": "VAL3"},
            ],
            "AE",
        ),
        (
            COMPLEX_DATA_AE,
            [
                {"studyid": "A", "domain": "AE", "usubjid": "A", "seq": 1, "cola": "VAL1", "colb": "VAL2"},
                {"studyid": "A", "domain": "AE", "usubjid": "A", "seq": 2, "cola": "VAL1", "colb": None},
                {"studyid": "A", "domain": "AE", "usubjid": "B", "seq": 1, "cola": None, "colb": None},
                {"studyid": "A", "domain": "AE", "usubjid": "C", "seq": 1, "cola": "VAL1", "colb": None},
            ],
            "AE",
        ),
    ],
)
def test_supp_merge(data, expected, domain, sdtm_standards_context):
    ds = PostgresQLDataService.instance()
    o_schema = PostgresQLDataService.add_test_dataset(ds, "original", data["original"], sdtm_standards_context)
    s_schema = PostgresQLDataService.add_test_dataset(ds, "supp", data["supp"], sdtm_standards_context)

    # Perform the join operation
    schema = SqlSuppMerge.perform_join(ds.pgi, o_schema, s_schema, domain)

    assert schema.get_column("STUDYID") is not None
    assert schema.get_column("COLA") is not None
    assert schema.get_column("COLB") is not None
    assert ds.pgi.schema.get_table(schema.name) == schema

    ds.pgi.execute_sql(
        f"""SELECT
                studyid,
                domain,
                usubjid,
                seq::int,
                {schema.get_column_hash("COLA")} AS cola,
                {schema.get_column_hash("COLB")} AS colb
            FROM {schema.hash} ORDER BY usubjid, seq"""
    )
    result = pd.DataFrame(ds.pgi.fetch_all())
    assert pd.DataFrame(expected).equals(result)


@pytest.mark.parametrize(
    "data",
    [SIMPLE_DATA_DM],
)
def test_merge_twice(data, sdtm_standards_context):
    ds = PostgresQLDataService.instance()
    o_schema = PostgresQLDataService.add_test_dataset(ds, "original", data["original"], sdtm_standards_context)
    s_schema = PostgresQLDataService.add_test_dataset(ds, "supp", data["supp"], sdtm_standards_context)

    # Perform the join operation
    schema = SqlSuppMerge.perform_join(ds.pgi, o_schema, s_schema, "DM")
    schema2 = SqlSuppMerge.perform_join(ds.pgi, o_schema, s_schema, "DM")
    assert schema == schema2


@pytest.mark.parametrize(
    "data",
    [SIMPLE_DATA_DM],
)
def test_original_base_column_missing(data, sdtm_standards_context):
    ds = PostgresQLDataService.instance()
    o_schema = PostgresQLDataService.add_test_dataset(ds, "original", data["original"], sdtm_standards_context)
    s_schema = PostgresQLDataService.add_test_dataset(ds, "supp", data["supp"], sdtm_standards_context)

    o_schema._columns.pop("usubjid", None)

    with pytest.raises(Exception) as e:
        SqlSuppMerge.perform_join(ds.pgi, o_schema, s_schema, "DM")
    assert "Original schema" in str(e.value)


@pytest.mark.parametrize(
    "data",
    [SIMPLE_DATA_DM],
)
def test_supp_base_column_missing(data, sdtm_standards_context):
    ds = PostgresQLDataService.instance()
    o_schema = PostgresQLDataService.add_test_dataset(ds, "original", data["original"], sdtm_standards_context)
    s_schema = PostgresQLDataService.add_test_dataset(ds, "supp", data["supp"], sdtm_standards_context)

    s_schema._columns.pop("rdomain", None)

    with pytest.raises(Exception) as e:
        SqlSuppMerge.perform_join(ds.pgi, o_schema, s_schema, "DM")
    assert "SUPP schema" in str(e.value)


@pytest.mark.parametrize(
    "data",
    [SIMPLE_DATA_DM],
)
def test_supp_idvar_column_missing(data, sdtm_standards_context):
    ds = PostgresQLDataService.instance()
    o_schema = PostgresQLDataService.add_test_dataset(ds, "original", data["original"], sdtm_standards_context)
    s_schema = PostgresQLDataService.add_test_dataset(ds, "supp", data["supp"], sdtm_standards_context)

    with pytest.raises(Exception) as e:
        SqlSuppMerge.perform_join(ds.pgi, o_schema, s_schema, "AE")
    assert "SUPP schema" in str(e.value)


@pytest.mark.parametrize(
    "data",
    [SIMPLE_DATA_AE],
)
def test_original_required_column_missing(data, sdtm_standards_context):
    ds = PostgresQLDataService.instance()
    o_schema = PostgresQLDataService.add_test_dataset(ds, "original", data["original"], sdtm_standards_context)
    s_schema = PostgresQLDataService.add_test_dataset(ds, "supp", data["supp"], sdtm_standards_context)

    o_schema._columns.pop("seq", None)

    with pytest.raises(Exception) as e:
        SqlSuppMerge.perform_join(ds.pgi, o_schema, s_schema, "AE")
    assert "Original schema" in str(e.value)


@pytest.mark.parametrize(
    "data",
    [SIMPLE_DATA_AE],
)
def test_original_qnam_column_already_exists(data, sdtm_standards_context):
    ds = PostgresQLDataService.instance()
    o_schema = PostgresQLDataService.add_test_dataset(ds, "original", data["original"], sdtm_standards_context)
    s_schema = PostgresQLDataService.add_test_dataset(ds, "supp", data["supp"], sdtm_standards_context)

    o_schema.add_column(SqlColumnSchema.generated(column="COLA", type="Char"))

    with pytest.raises(Exception) as e:
        SqlSuppMerge.perform_join(ds.pgi, o_schema, s_schema, "AE")
    assert "already exists" in str(e.value)
