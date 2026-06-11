import pandas as pd
import pytest

from cdisc_rules_engine.data_service.merges.relationship import SqlRelationshipMerge
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)

SIMPLE_RELATIONSHIP_DATA = {
    "original": {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001"],
        "DOMAIN": ["EC", "EC", "EC"],
        "USUBJID": ["SUBJ001", "SUBJ001", "SUBJ002"],
        "ECSEQ": [1, 2, 1],
        "ECSTDY": [1, 5, 10],
    },
    "relationship": {
        "STUDYID": ["STUDY001", "STUDY001"],
        "RDOMAIN": ["EC", "EC"],
        "USUBJID": ["SUBJ001", "SUBJ001"],
        "IDVAR": ["ECSEQ", "ECSEQ"],
        "IDVARVAL": ["1", "2"],
        "POOLID": ["POOL1", "POOL2"],
    },
}

EMPTY_RELATIONSHIP_DATA = {
    "original": {
        "STUDYID": ["STUDY001", "STUDY001"],
        "DOMAIN": ["EC", "EC"],
        "USUBJID": ["SUBJ001", "SUBJ002"],
        "ECSEQ": [1, 1],
        "ECSTDY": [1, 10],
    },
    "relationship": {
        "STUDYID": ["STUDY001", "STUDY001"],
        "RDOMAIN": ["EC", "EC"],
        "USUBJID": ["SUBJ001", "SUBJ002"],
        "IDVAR": ["", ""],
        "IDVARVAL": ["", ""],
        "POOLID": ["POOL1", "POOL2"],
    },
}

COMPLEX_RELATIONSHIP_DATA = {
    "original": {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001"],
        "DOMAIN": ["AE", "AE", "AE"],
        "USUBJID": ["SUBJ001", "SUBJ001", "SUBJ002"],
        "AESEQ": [1, 2, 1],
        "AESTDY": [2, 7, 12],
        "AESEV": ["MILD", "SEVERE", "MODERATE"],
    },
    "relationship": {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001"],
        "RDOMAIN": ["AE", "AE", "AE"],
        "USUBJID": ["SUBJ001", "SUBJ001", "SUBJ002"],
        "IDVAR": ["AESEQ", "AESEV", "AESEQ"],
        "IDVARVAL": ["1", "MILD", "1"],
        "COREFID": ["REF001", "REF002", "REF003"],
        "COREF": ["Reference comment 1", "Severity comment", "Reference comment 3"],
    },
}

MIXED_RDOMAIN_DATA = {
    "original": {
        "STUDYID": ["STUDY001", "STUDY001"],
        "DOMAIN": ["EC", "EC"],
        "USUBJID": ["SUBJ001", "SUBJ002"],
        "ECSEQ": [1, 1],
        "ECSTDY": [5, 10],
    },
    "relationship": {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001"],
        "RDOMAIN": ["EC", "EC", "AE"],
        "USUBJID": ["SUBJ001", "SUBJ002", "SUBJ003"],
        "IDVAR": ["", "", ""],
        "IDVARVAL": ["", "", ""],
        "POOLID": ["POOL1", "POOL2", "POOL3"],
    },
}


@pytest.mark.parametrize(
    "data, expected, domain",
    [
        (
            SIMPLE_RELATIONSHIP_DATA,
            [
                {
                    "studyid": "STUDY001",
                    "domain": "EC",
                    "usubjid": "SUBJ001",
                    "ecseq": 1,
                    "ecstdy": 1,
                    "poolid": "POOL1",
                },
                {
                    "studyid": "STUDY001",
                    "domain": "EC",
                    "usubjid": "SUBJ001",
                    "ecseq": 2,
                    "ecstdy": 5,
                    "poolid": "POOL2",
                },
            ],
            "RELSUB",
        ),
        (
            EMPTY_RELATIONSHIP_DATA,
            [
                {
                    "studyid": "STUDY001",
                    "domain": "EC",
                    "usubjid": "SUBJ001",
                    "ecseq": 1,
                    "ecstdy": 1,
                    "poolid": "POOL1",
                },
                {
                    "studyid": "STUDY001",
                    "domain": "EC",
                    "usubjid": "SUBJ002",
                    "ecseq": 1,
                    "ecstdy": 10,
                    "poolid": "POOL2",
                },
            ],
            "RELSUB",
        ),
        (
            MIXED_RDOMAIN_DATA,
            [
                {
                    "studyid": "STUDY001",
                    "domain": "EC",
                    "usubjid": "SUBJ001",
                    "ecseq": 1.0,
                    "ecstdy": 5.0,
                    "poolid": "POOL1",
                },
                {
                    "studyid": "STUDY001",
                    "domain": "EC",
                    "usubjid": "SUBJ002",
                    "ecseq": 1.0,
                    "ecstdy": 10.0,
                    "poolid": "POOL2",
                },
                {
                    "studyid": None,
                    "domain": None,
                    "usubjid": None,
                    "ecseq": None,
                    "ecstdy": None,
                    "poolid": "POOL3",
                },
            ],
            "RELSUB",
        ),
    ],
)
def test_relationship_merge(data, expected, domain, sdtm_standards_context):
    """Test relationship merge functionality with expected data comparison."""
    data_service = PostgresQLDataService.instance()
    original_schema = PostgresQLDataService.add_test_dataset(
        data_service, "original", data["original"], sdtm_standards_context
    )
    relationship_schema = PostgresQLDataService.add_test_dataset(
        data_service, "relationship", data["relationship"], sdtm_standards_context
    )

    relationship_columns = {"column_with_names": "IDVAR", "column_with_values": "IDVARVAL"}

    result = SqlRelationshipMerge.perform_join(
        pgi=data_service.pgi,
        original=original_schema,
        relationship_dataset=relationship_schema,
        domain=domain,
        relationship_columns=relationship_columns,
    )

    # Verify schema has expected columns
    assert result.get_column("STUDYID") is not None
    assert result.get_column("DOMAIN") is not None
    assert result.get_column("USUBJID") is not None
    assert result.get_column(f"POOLID.{domain}") is not None
    assert data_service.pgi.schema.get_table(result.name) == result

    # Get actual results and compare with expected
    data_service.pgi.execute_sql(
        f"""SELECT
                studyid,
                domain,
                usubjid,
                ecseq::int,
                ecstdy::int,
                {result.get_column_hash(f"POOLID.{domain}")} AS poolid
            FROM {result.hash} ORDER BY usubjid, ecseq"""
    )
    actual_results = pd.DataFrame(data_service.pgi.fetch_all())
    expected_df = pd.DataFrame(expected)
    assert expected_df.equals(actual_results), f"Expected {expected}, got {actual_results.to_dict('records')}"


def test_merge_twice(sdtm_standards_context):
    """Test that running the same merge twice returns cached result."""
    data_service = PostgresQLDataService.instance()
    original_schema = PostgresQLDataService.add_test_dataset(
        data_service, "original", SIMPLE_RELATIONSHIP_DATA["original"], sdtm_standards_context
    )
    relationship_schema = PostgresQLDataService.add_test_dataset(
        data_service, "relationship", SIMPLE_RELATIONSHIP_DATA["relationship"], sdtm_standards_context
    )

    relationship_columns = {"column_with_names": "IDVAR", "column_with_values": "IDVARVAL"}

    result1 = SqlRelationshipMerge.perform_join(
        pgi=data_service.pgi,
        original=original_schema,
        relationship_dataset=relationship_schema,
        domain="RELSUB",
        relationship_columns=relationship_columns,
    )

    result2 = SqlRelationshipMerge.perform_join(
        pgi=data_service.pgi,
        original=original_schema,
        relationship_dataset=relationship_schema,
        domain="RELSUB",
        relationship_columns=relationship_columns,
    )

    assert result1 == result2


def test_validation_missing_original_columns(sdtm_standards_context):
    """Test validation when required columns are missing in original dataset."""
    data_service = PostgresQLDataService.instance()
    original_schema = PostgresQLDataService.add_test_dataset(
        data_service, "original", SIMPLE_RELATIONSHIP_DATA["original"], sdtm_standards_context
    )
    relationship_schema = PostgresQLDataService.add_test_dataset(
        data_service, "relationship", SIMPLE_RELATIONSHIP_DATA["relationship"], sdtm_standards_context
    )

    # Remove required column
    original_schema._columns.pop("usubjid", None)

    relationship_columns = {"column_with_names": "IDVAR", "column_with_values": "IDVARVAL"}

    with pytest.raises(ValueError, match="Original schema missing match key column"):
        SqlRelationshipMerge.perform_join(
            pgi=data_service.pgi,
            original=original_schema,
            relationship_dataset=relationship_schema,
            domain="RELSUB",
            relationship_columns=relationship_columns,
        )


def test_validation_missing_relationship_columns(sdtm_standards_context):
    """Test that missing relationship columns are handled gracefully with simple merge."""
    data_service = PostgresQLDataService.instance()
    original_schema = PostgresQLDataService.add_test_dataset(
        data_service, "original", SIMPLE_RELATIONSHIP_DATA["original"], sdtm_standards_context
    )
    relationship_schema = PostgresQLDataService.add_test_dataset(
        data_service, "relationship", SIMPLE_RELATIONSHIP_DATA["relationship"], sdtm_standards_context
    )

    relationship_schema._columns.pop("idvar", None)

    relationship_columns = {"column_with_names": "IDVAR", "column_with_values": "IDVARVAL"}

    result = SqlRelationshipMerge.perform_join(
        pgi=data_service.pgi,
        original=original_schema,
        relationship_dataset=relationship_schema,
        domain="RELSUB",
        relationship_columns=relationship_columns,
    )

    assert "SIMPLE" in result.name.upper()


def test_complex_multi_column_filtering(sdtm_standards_context):
    """Test filtering with multiple IDVAR columns."""
    data_service = PostgresQLDataService.instance()
    original_schema = PostgresQLDataService.add_test_dataset(
        data_service, "original", COMPLEX_RELATIONSHIP_DATA["original"], sdtm_standards_context
    )
    relationship_schema = PostgresQLDataService.add_test_dataset(
        data_service, "relationship", COMPLEX_RELATIONSHIP_DATA["relationship"], sdtm_standards_context
    )

    relationship_columns = {"column_with_names": "IDVAR", "column_with_values": "IDVARVAL"}

    result = SqlRelationshipMerge.perform_join(
        pgi=data_service.pgi,
        original=original_schema,
        relationship_dataset=relationship_schema,
        domain="CO",
        relationship_columns=relationship_columns,
    )

    assert result.has_column("COREFID.CO")
    assert result.has_column("COREF.CO")

    data_service.pgi.execute_sql(
        f"""SELECT
                usubjid,
                aeseq::int,
                aesev,
                {result.get_column_hash("COREF.CO")} as coref
            FROM {result.hash}
            WHERE {result.get_column_hash("COREF.CO")} IS NOT NULL
            AND usubjid IS NOT NULL
            ORDER BY aeseq"""
    )
    results = data_service.pgi.fetch_all()

    assert len(results) >= 1
    assert results[0]["aeseq"] == 1 and results[0]["aesev"] == "MILD"


def test_validation_invalid_parameters(sdtm_standards_context):
    """Test validation with invalid parameters."""
    data_service = PostgresQLDataService.instance()
    original_schema = PostgresQLDataService.add_test_dataset(
        data_service, "original", SIMPLE_RELATIONSHIP_DATA["original"], sdtm_standards_context
    )
    relationship_schema = PostgresQLDataService.add_test_dataset(
        data_service, "relationship", SIMPLE_RELATIONSHIP_DATA["relationship"], sdtm_standards_context
    )

    with pytest.raises(ValueError, match="relationship_columns parameter is required"):
        SqlRelationshipMerge.perform_join(
            pgi=data_service.pgi,
            original=original_schema,
            relationship_dataset=relationship_schema,
            domain="RELSUB",
            relationship_columns=None,
        )

    with pytest.raises(ValueError, match="column_with_names is required"):
        SqlRelationshipMerge.perform_join(
            pgi=data_service.pgi,
            original=original_schema,
            relationship_dataset=relationship_schema,
            domain="RELSUB",
            relationship_columns={"column_with_names": "", "column_with_values": "IDVARVAL"},
        )
