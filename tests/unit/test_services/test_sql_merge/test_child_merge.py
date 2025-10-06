"""
Tests for SQL child merge implementation.
"""

import pandas as pd
import pytest

from cdisc_rules_engine.data_service.merges.child import SqlChildMerge
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata


SIMPLE_RDOMAIN_DATA = {
    "child": {
        "STUDYID": ["S1", "S1", "S1"],
        "RDOMAIN": ["AE", "AE", "AE"],
        "USUBJID": ["U1", "U2", "U3"],
        "IDVAR": ["AESEQ", "AESEQ", "AESEQ"],
        "IDVARVAL": ["1", "1", "1"],
        "QNAM": ["SEV", "SEV", "SEV"],
        "QVAL": ["MILD", "MODERATE", "SEVERE"],
    },
    "parent": {
        "STUDYID": ["S1", "S1"],
        "USUBJID": ["U1", "U2"],
        "AESEQ": ["1", "1"],
        "AETERM": ["Headache", "Nausea"],
        "AGE": ["45", "52"],
    },
}

PATTERN_REPLACEMENT_DATA = {
    "child": {
        "STUDYID": ["S1", "S1"],
        "RDOMAIN": ["LB", "LB"],
        "USUBJID": ["U1", "U2"],
        "IDVAR": ["LBSEQ", "LBSEQ"],
        "IDVARVAL": ["1", "2"],
        "QNAM": ["REPEAT", "REPEAT"],
    },
    "parent": {
        "STUDYID": ["S1", "S1"],
        "USUBJID": ["U1", "U2"],
        "LBSEQ": ["1", "2"],
        "LBTEST": ["Glucose", "Sodium"],
    },
}

MATCH_KEY_FALLBACK_DATA = {
    "child": {
        "STUDYID": ["S1", "S1"],
        "USUBJID": ["U1", "U2"],
        "SEQ": ["1", "2"],
        "CHILDCOL": ["A", "B"],
    },
    "parent": {
        "STUDYID": ["S1", "S1"],
        "USUBJID": ["U1", "U2"],
        "SEQ": ["1", "2"],
        "PARENTCOL": ["X", "Y"],
    },
}


@pytest.mark.parametrize(
    "data, expected, child_domain",
    [
        (
            SIMPLE_RDOMAIN_DATA,
            [
                {
                    "studyid": "S1",
                    "rdomain": "AE",
                    "usubjid": "U1",
                    "qnam": "SEV",
                    "qval": "MILD",
                    "aeterm": "Headache",
                    "age": "45",
                },
                {
                    "studyid": "S1",
                    "rdomain": "AE",
                    "usubjid": "U2",
                    "qnam": "SEV",
                    "qval": "MODERATE",
                    "aeterm": "Nausea",
                    "age": "52",
                },
                {
                    "studyid": "S1",
                    "rdomain": "AE",
                    "usubjid": "U3",
                    "qnam": "SEV",
                    "qval": "SEVERE",
                    "aeterm": None,
                    "age": None,
                },
            ],
            "SUPPAE",
        ),
    ],
)
def test_child_merge_with_rdomain(data, expected, child_domain):
    """Test basic child merge using RDOMAIN column to find parent."""
    ds = PostgresQLDataService.instance()

    child_schema = PostgresQLDataService.add_test_dataset(ds, "suppae", data["child"])
    PostgresQLDataService.add_test_dataset(ds, "ae", data["parent"])

    first_record = {k: v[0] if v else None for k, v in data["parent"].items()}
    first_record["DOMAIN"] = "AE"
    datasets = [
        SDTMDatasetMetadata(
            name="ae",
            filename="ae.xpt",
            label="Adverse Events",
            full_path="/test/ae.xpt",
            file_size=100,
            record_count=len(data["parent"]["STUDYID"]),
            modification_date=None,
            first_record=first_record,
        )
    ]

    merge_spec = {"match_key": ["STUDYID", "USUBJID", {"left": "IDVARVAL", "right": "AESEQ"}]}

    result_schema = SqlChildMerge.perform_merge(
        pgi=ds.pgi,
        child=child_schema,
        child_domain=child_domain,
        datasets=datasets,
        merge_spec=merge_spec,
    )

    assert result_schema.has_column("studyid")
    assert result_schema.has_column("rdomain")
    assert result_schema.has_column("usubjid")
    assert result_schema.has_column("qnam")
    assert result_schema.has_column("qval")
    assert result_schema.has_column("ae.aeterm") or result_schema.has_column("aeterm")
    assert result_schema.has_column("ae.age") or result_schema.has_column("age")

    aeterm_col = result_schema.get_column_hash("aeterm")
    age_col = result_schema.get_column_hash("age")

    ds.pgi.execute_sql(
        f"""SELECT
                studyid,
                rdomain,
                usubjid,
                qnam,
                qval,
                {aeterm_col} as aeterm,
                {age_col} as age
            FROM {result_schema.hash}
            ORDER BY usubjid"""
    )
    actual = pd.DataFrame(ds.pgi.fetch_all())
    expected_df = pd.DataFrame(expected)

    assert expected_df.equals(actual), f"Expected:\n{expected_df}\n\nActual:\n{actual}"


def test_child_merge_with_pattern_replacement():
    """Test child merge with -- pattern replacement in match keys."""
    ds = PostgresQLDataService.instance()
    data = PATTERN_REPLACEMENT_DATA

    child_schema = PostgresQLDataService.add_test_dataset(ds, "supplb", data["child"])
    PostgresQLDataService.add_test_dataset(ds, "lb", data["parent"])

    first_record = {k: v[0] for k, v in data["parent"].items()}
    first_record["DOMAIN"] = "LB"
    datasets = [
        SDTMDatasetMetadata(
            name="lb",
            filename="lb.xpt",
            label="Labs",
            full_path="/test/lb.xpt",
            file_size=100,
            record_count=2,
            modification_date=None,
            first_record=first_record,
        )
    ]

    merge_spec = {"match_key": ["STUDYID", "USUBJID", {"left": "IDVARVAL", "right": "--SEQ"}]}

    result_schema = SqlChildMerge.perform_merge(
        pgi=ds.pgi,
        child=child_schema,
        child_domain="SUPPLB",
        datasets=datasets,
        merge_spec=merge_spec,
    )

    assert result_schema is not None
    ds.pgi.execute_sql(f"SELECT COUNT(*) as count FROM {result_schema.hash}")
    result = ds.pgi.fetch_all()
    assert result[0]["count"] == 2

    lbtest_col = result_schema.get_column_hash("lbtest")
    ds.pgi.execute_sql(
        f"""SELECT
                usubjid,
                {lbtest_col} as lbtest
            FROM {result_schema.hash}
            ORDER BY usubjid"""
    )
    results = ds.pgi.fetch_all()
    assert len(results) == 2
    assert results[0]["lbtest"] == "Glucose"
    assert results[1]["lbtest"] == "Sodium"


def test_child_merge_match_key_fallback():
    """Test child merge using match key fallback (no RDOMAIN column)."""
    ds = PostgresQLDataService.instance()
    data = MATCH_KEY_FALLBACK_DATA

    child_schema = PostgresQLDataService.add_test_dataset(ds, "child", data["child"])
    PostgresQLDataService.add_test_dataset(ds, "parent", data["parent"])

    first_record = {k: v[0] for k, v in data["parent"].items()}
    first_record["DOMAIN"] = "PARENT"
    datasets = [
        SDTMDatasetMetadata(
            name="parent",
            filename="parent.xpt",
            label="Parent",
            full_path="/test/parent.xpt",
            file_size=100,
            record_count=2,
            modification_date=None,
            first_record=first_record,
        )
    ]

    merge_spec = {"match_key": ["STUDYID", "USUBJID", "SEQ"]}

    result_schema = SqlChildMerge.perform_merge(
        pgi=ds.pgi,
        child=child_schema,
        child_domain="CHILD",
        datasets=datasets,
        merge_spec=merge_spec,
    )

    assert result_schema is not None
    ds.pgi.execute_sql(f"SELECT COUNT(*) as count FROM {result_schema.hash}")
    result = ds.pgi.fetch_all()
    assert result[0]["count"] == 2

    parentcol_col = result_schema.get_column_hash("parentcol")
    ds.pgi.execute_sql(
        f"""SELECT
                childcol,
                {parentcol_col} as parentcol
            FROM {result_schema.hash}
            ORDER BY childcol"""
    )
    results = ds.pgi.fetch_all()
    assert results[0]["childcol"] == "A"
    assert results[0]["parentcol"] == "X"
    assert results[1]["childcol"] == "B"
    assert results[1]["parentcol"] == "Y"


def test_child_merge_run_twice():
    """Test that running same child merge twice returns cached result."""
    ds = PostgresQLDataService.instance()
    data = SIMPLE_RDOMAIN_DATA

    child_schema = PostgresQLDataService.add_test_dataset(ds, "suppae", data["child"])
    PostgresQLDataService.add_test_dataset(ds, "ae", data["parent"])

    first_record = {k: v[0] for k, v in data["parent"].items()}
    first_record["DOMAIN"] = "AE"
    datasets = [
        SDTMDatasetMetadata(
            name="ae",
            filename="ae.xpt",
            label="Adverse Events",
            full_path="/test/ae.xpt",
            file_size=100,
            record_count=2,
            modification_date=None,
            first_record=first_record,
        )
    ]

    merge_spec = {"match_key": ["STUDYID", "USUBJID", {"left": "IDVARVAL", "right": "AESEQ"}]}

    result1 = SqlChildMerge.perform_merge(
        pgi=ds.pgi,
        child=child_schema,
        child_domain="SUPPAE",
        datasets=datasets,
        merge_spec=merge_spec,
    )

    result2 = SqlChildMerge.perform_merge(
        pgi=ds.pgi,
        child=child_schema,
        child_domain="SUPPAE",
        datasets=datasets,
        merge_spec=merge_spec,
    )

    assert result1 == result2
    assert result1.name == result2.name


def test_child_merge_no_parent_found():
    """Test that error is raised when no parent dataset is found."""
    ds = PostgresQLDataService.instance()

    child_data = {
        "STUDYID": ["S1"],
        "RDOMAIN": ["NONEXISTENT"],
        "USUBJID": ["U1"],
    }
    child_schema = PostgresQLDataService.add_test_dataset(ds, "child", child_data)

    datasets = []

    merge_spec = {"match_key": ["STUDYID", "USUBJID"]}

    with pytest.raises(ValueError, match="Could not find parent dataset"):
        SqlChildMerge.perform_merge(
            pgi=ds.pgi,
            child=child_schema,
            child_domain="CHILD",
            datasets=datasets,
            merge_spec=merge_spec,
        )


def test_child_merge_unmatched_child_rows():
    """Test that unmatched child rows are preserved with NULL parent values (LEFT JOIN)."""
    ds = PostgresQLDataService.instance()

    child_data = {
        "STUDYID": ["S1", "S1", "S1"],
        "RDOMAIN": ["AE", "AE", "AE"],
        "USUBJID": ["U1", "U2", "U3"],
        "IDVAR": ["AESEQ", "AESEQ", "AESEQ"],
        "IDVARVAL": ["1", "1", "1"],
    }
    parent_data = {
        "STUDYID": ["S1"],
        "USUBJID": ["U1"],
        "AESEQ": ["1"],
        "AETERM": ["Headache"],
    }

    child_schema = PostgresQLDataService.add_test_dataset(ds, "suppae", child_data)
    PostgresQLDataService.add_test_dataset(ds, "ae", parent_data)

    first_record = {k: v[0] for k, v in parent_data.items()}
    first_record["DOMAIN"] = "AE"
    datasets = [
        SDTMDatasetMetadata(
            name="ae",
            filename="ae.xpt",
            label="Adverse Events",
            full_path="/test/ae.xpt",
            file_size=100,
            record_count=1,
            modification_date=None,
            first_record=first_record,
        )
    ]

    merge_spec = {"match_key": ["STUDYID", "USUBJID", {"left": "IDVARVAL", "right": "AESEQ"}]}

    result_schema = SqlChildMerge.perform_merge(
        pgi=ds.pgi,
        child=child_schema,
        child_domain="SUPPAE",
        datasets=datasets,
        merge_spec=merge_spec,
    )

    ds.pgi.execute_sql(f"SELECT COUNT(*) as count FROM {result_schema.hash}")
    result = ds.pgi.fetch_all()
    assert result[0]["count"] == 3

    aeterm_col = result_schema.get_column_hash("aeterm")
    ds.pgi.execute_sql(
        f"""SELECT
                usubjid,
                {aeterm_col} as aeterm
            FROM {result_schema.hash}
            WHERE usubjid IN ('U2', 'U3')
            ORDER BY usubjid"""
    )
    unmatched = ds.pgi.fetch_all()
    assert len(unmatched) == 2
    assert unmatched[0]["aeterm"] is None
    assert unmatched[1]["aeterm"] is None
