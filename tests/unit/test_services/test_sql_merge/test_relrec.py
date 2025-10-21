import pytest

from cdisc_rules_engine.data_service.merges.relrec import SqlRelrecMerge
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)

SIMPLE_RELREC_DATA = {
    "original": {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001"],
        "USUBJID": ["SUBJ001", "SUBJ001", "SUBJ002"],
        "ECSEQ": [1, 2, 1],
        "ECSTDY": [1, 5, 10],
    },
    "relrec": {
        "STUDYID": ["STUDY001", "STUDY001"],
        "USUBJID": ["SUBJ001", "SUBJ001"],
        "RELID": ["REL001", "REL001"],
        "RDOMAIN": ["EC", "AE"],
        "IDVAR": ["ECSEQ", "AESEQ"],
        "IDVARVAL": ["1", "1"],
    },
    "ae": {
        "STUDYID": ["STUDY001"],
        "USUBJID": ["SUBJ001"],
        "AESEQ": [1],
        "AESTDY": [2],
    },
}

COMPLEX_RELREC_DATA = {
    "original": {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001", "STUDY001"],
        "USUBJID": ["SUBJ001", "SUBJ001", "SUBJ002", "SUBJ002"],
        "ECSEQ": [1, 2, 1, 2],
        "ECSTDY": [1, 5, 10, 15],
    },
    "relrec": {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001", "STUDY001"],
        "USUBJID": ["SUBJ001", "SUBJ001", "SUBJ002", "SUBJ002"],
        "RELID": ["REL001", "REL001", "REL002", "REL002"],
        "RDOMAIN": ["EC", "AE", "EC", "DM"],
        "IDVAR": ["ECSEQ", "AESEQ", "ECSEQ", ""],
        "IDVARVAL": ["1", "1", "1", ""],
    },
    "ae": {
        "STUDYID": ["STUDY001"],
        "USUBJID": ["SUBJ001"],
        "AESEQ": [1],
        "AESTDY": [2],
    },
    "dm": {
        "STUDYID": ["STUDY001"],
        "USUBJID": ["SUBJ002"],
        "AGE": [30],
    },
}


@pytest.mark.parametrize(
    "data, expected_columns",
    [
        (
            SIMPLE_RELREC_DATA,
            [
                "studyid",
                "usubjid",
                "ecseq",
                "ecstdy",
                "relrec.studyid",
                "relrec.usubjid",
                "relrec.__seq",
                "relrec.__stdy",
            ],
        ),
        (
            COMPLEX_RELREC_DATA,
            [
                "studyid",
                "usubjid",
                "ecseq",
                "ecstdy",
                "relrec.studyid",
                "relrec.usubjid",
                "relrec.__seq",
                "relrec.__stdy",
                "relrec.age",
            ],
        ),
    ],
)
def test_relrec_merge_column_structure(data, expected_columns, sdtm_standards_context):
    """Test that RELREC merge creates the correct column structure."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    # Create and populate original table
    original_schema = PostgresQLDataService.add_test_dataset(ds, "ec", data["original"], sdtm_standards_context)

    # Create and populate RELREC table
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", data["relrec"], sdtm_standards_context)

    # Create related domain tables
    for domain, domain_data in data.items():
        if domain not in ["original", "relrec"]:
            PostgresQLDataService.add_test_dataset(ds, domain, domain_data, sdtm_standards_context)

    # Perform RELREC merge
    result_schema = SqlRelrecMerge.perform_join(
        pgi=pgi,
        original=original_schema,
        relrec=relrec_schema,
        domain="EC",
        wildcard="__",
    )

    # Check that expected columns are present
    for expected_col in expected_columns:
        assert result_schema.has_column(expected_col), f"Missing expected column: {expected_col}"


def test_relrec_merge_basic_functionality(sdtm_standards_context):
    """Test basic RELREC merge functionality with simple data."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi
    data = SIMPLE_RELREC_DATA

    # Create and populate tables
    original_schema = PostgresQLDataService.add_test_dataset(ds, "ec", data["original"], sdtm_standards_context)
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", data["relrec"], sdtm_standards_context)

    PostgresQLDataService.add_test_dataset(ds, "ae", data["ae"], sdtm_standards_context)

    # Perform RELREC merge
    result_schema = SqlRelrecMerge.perform_join(
        pgi=pgi,
        original=original_schema,
        relrec=relrec_schema,
        domain="EC",
        wildcard="__",
    )

    # Check that result table was created and has data
    pgi.execute_sql(f"SELECT COUNT(*) as count FROM {result_schema.hash}")
    result = pgi.fetch_all()
    assert result[0]["count"] >= 1, "Result table should have at least one row"


def test_relrec_merge_no_relationships(sdtm_standards_context):
    """Test RELREC merge when no relationships exist for the domain."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    # Create original data
    original_data = {
        "STUDYID": ["STUDY001"],
        "USUBJID": ["SUBJ001"],
        "ECSEQ": [1],
        "ECSTDY": [1],
    }
    original_schema = PostgresQLDataService.add_test_dataset(ds, "ec", original_data, sdtm_standards_context)

    # Create RELREC with no EC relationships
    relrec_data = {
        "STUDYID": ["STUDY001"],
        "USUBJID": ["SUBJ001"],
        "RELID": ["REL001"],
        "RDOMAIN": ["AE"],  # No EC domain
        "IDVAR": ["AESEQ"],
        "IDVARVAL": ["1"],
    }
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", relrec_data, sdtm_standards_context)

    # Perform RELREC merge
    result_schema = SqlRelrecMerge.perform_join(
        pgi=pgi,
        original=original_schema,
        relrec=relrec_schema,
        domain="EC",
        wildcard="__",
    )

    # Should have same data as original since no relationships
    pgi.execute_sql(f"SELECT COUNT(*) as count FROM {result_schema.hash}")
    result = pgi.fetch_all()
    assert result[0]["count"] == 1, "Should have original data when no relationships exist"


def test_relrec_merge_wildcard_renaming(sdtm_standards_context):
    """Test that wildcard renaming works correctly."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi
    data = SIMPLE_RELREC_DATA

    # Create tables
    original_schema = PostgresQLDataService.add_test_dataset(ds, "ec", data["original"], sdtm_standards_context)
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", data["relrec"], sdtm_standards_context)

    PostgresQLDataService.add_test_dataset(ds, "ae", data["ae"], sdtm_standards_context)

    # Test different wildcard values
    for wildcard in ["__", "XX", "**"]:
        result_schema = SqlRelrecMerge.perform_join(
            pgi=pgi,
            original=original_schema,
            relrec=relrec_schema,
            domain="EC",
            wildcard=wildcard,
        )

        # Check that wildcard is correctly applied (note: column names are lowercase in PostgreSQL)
        expected_col = f"relrec.{wildcard.lower()}stdy"
        assert result_schema.has_column(expected_col), f"Missing wildcard column: {expected_col}"


def test_relrec_validation_errors(sdtm_standards_context):
    """Test that validation errors are properly raised."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    # Create table without required columns
    invalid_original_data = {
        "DOMAIN": ["EC"],  # Missing STUDYID, USUBJID
        "ECSEQ": [1],
    }
    invalid_original_schema = PostgresQLDataService.add_test_dataset(
        ds, "ec_invalid", invalid_original_data, sdtm_standards_context
    )

    relrec_data = {
        "STUDYID": ["STUDY001"],
        "USUBJID": ["SUBJ001"],
        "RELID": ["REL001"],
        "RDOMAIN": ["EC"],
    }
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", relrec_data, sdtm_standards_context)

    # Should raise validation error
    with pytest.raises(ValueError, match="Original schema is missing required column"):
        SqlRelrecMerge.perform_join(
            pgi=pgi,
            original=invalid_original_schema,
            relrec=relrec_schema,
            domain="EC",
            wildcard="__",
        )


def test_relrec_filter_for_domain(sdtm_standards_context):
    """Test the _filter_relrec_for_domain method."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    relrec_data = {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001", "STUDY001"],
        "USUBJID": ["SUBJ001", "SUBJ001", "SUBJ002", "SUBJ002"],
        "RELID": ["REL001", "REL001", "REL002", "REL002"],
        "RDOMAIN": ["EC", "AE", "EC", "DM"],
        "IDVAR": ["ECSEQ", "AESEQ", "ECSEQ", ""],
        "IDVARVAL": ["1", "1", "1", ""],
    }
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", relrec_data, sdtm_standards_context)

    # Filter for EC domain
    relationships = SqlRelrecMerge._filter_relrec_for_domain(pgi, relrec_schema, "EC")

    # Should return relationships where left side is EC and right side is not EC
    assert len(relationships) == 2, "Should find 2 relationships for EC domain"

    # Check that we have the expected relationships
    right_domains = [rel["rdomain_right"] for rel in relationships]
    assert "AE" in right_domains, "Should include AE relationship"
    assert "DM" in right_domains, "Should include DM relationship"
    assert "EC" not in right_domains, "Should not include EC as right domain"


def test_relrec_apply_wildcard_renaming(sdtm_standards_context):
    """Test the _apply_wildcard_renaming method."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    # Create a test domain table
    domain_data = {
        "STUDYID": ["STUDY001"],
        "USUBJID": ["SUBJ001"],
        "AESEQ": [1],
        "AESTDY": [1],
        "AETERM": ["Headache"],
    }
    domain_schema = PostgresQLDataService.add_test_dataset(ds, "ae", domain_data, sdtm_standards_context)

    # Test wildcard renaming
    renamed_columns = SqlRelrecMerge._apply_wildcard_renaming(pgi, domain_schema, "AE", "__")

    # Check expected renamings (note: column names are lowercase in PostgreSQL)
    assert "aestdy" in renamed_columns, "AESTDY should be in renamed columns"
    assert renamed_columns["aestdy"] == "RELREC.__STDY", "AESTDY should be renamed to RELREC.__STDY"

    assert "aeseq" in renamed_columns, "AESEQ should be in renamed columns"
    assert renamed_columns["aeseq"] == "RELREC.__SEQ", "AESEQ should be renamed to RELREC.__SEQ"

    assert "aeterm" in renamed_columns, "AETERM should be in renamed columns"
    assert renamed_columns["aeterm"] == "RELREC.aeterm", "AETERM should keep original name with RELREC prefix"


def test_relrec_merge_cg0601_scenario(sdtm_standards_context):
    """Test the specific CG0601 scenario with data type mismatch (string vs integer)."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    # Create AE data matching CG0601 test case
    ae_data = {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001"],
        "USUBJID": ["CDISC001", "CDISC001", "CDISC001"],
        "AESEQ": [1, 2, 3],
        "AEGRPID": ["1", "2", "3"],  # String values like in Excel
        "AETERM": ["HEADACHE", "FATIGUE", "NAUSEA"],
        "AEDECOD": ["Headache", None, "Nausea"],  # Row 2 has null AEDECOD
    }
    ae_schema = PostgresQLDataService.add_test_dataset(ds, "ae", ae_data, sdtm_standards_context)

    # Create FA data matching CG0601 test case
    fa_data = {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001"],
        "USUBJID": ["CDISC001", "CDISC001", "CDISC001"],
        "FASEQ": [1, 2, 3],
        "FAGRPID": [1, 2, 3],  # Integer values like in Excel
        "FAOBJ": ["OK", "ERROR", "WARNING"],  # Row 2 has "ERROR"
        "FATEST": ["Severity", "Severity", "Severity"],
    }
    PostgresQLDataService.add_test_dataset(ds, "fa", fa_data, sdtm_standards_context)

    # Create RELREC data matching CG0601 test case
    relrec_data = {
        "STUDYID": ["STUDY001", "STUDY001"],
        "USUBJID": ["CDISC001", "CDISC001"],
        "RELID": ["AEFA", "AEFA"],
        "RDOMAIN": ["AE", "FA"],
        "IDVAR": ["AEGRPID", "FAGRPID"],
        "IDVARVAL": ["", ""],  # Empty values - should join on IDVAR columns directly
    }
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", relrec_data, sdtm_standards_context)

    # Perform RELREC merge
    result_schema = SqlRelrecMerge.perform_join(
        pgi=pgi,
        original=ae_schema,
        relrec=relrec_schema,
        domain="AE",
        wildcard="FA",
    )

    # Check that the merge created the expected columns
    assert result_schema.has_column("relrec.faobj"), "Should have RELREC.FAOBJ column"

    # Check that the data was joined correctly
    pgi.execute_sql(
        f"""
        SELECT
            {ae_schema.get_column_hash('USUBJID')} as usubjid,
            {ae_schema.get_column_hash('AESEQ')} as aeseq,
            {ae_schema.get_column_hash('AETERM')} as aeterm,
            {ae_schema.get_column_hash('AEDECOD')} as aedecod,
            {result_schema.get_column_hash('relrec.faobj')} as faobj
        FROM {result_schema.hash}
        WHERE {ae_schema.get_column_hash('AESEQ')} = 2
    """
    )
    result = pgi.fetch_all()

    # Should find the relationship: AESEQ=2, AETERM="FATIGUE", AEDECOD=null, FAOBJ="ERROR"
    assert len(result) == 1, f"Should find exactly 1 record for AESEQ=2, got {len(result)}"

    record = result[0]
    assert record["usubjid"] == "CDISC001", f"Expected USUBJID=CDISC001, got {record['usubjid']}"
    assert record["aeseq"] == 2, f"Expected AESEQ=2, got {record['aeseq']}"
    assert record["aeterm"] == "FATIGUE", f"Expected AETERM=FATIGUE, got {record['aeterm']}"
    assert record["aedecod"] is None, f"Expected AEDECOD=null, got {record['aedecod']}"
    assert record["faobj"] == "ERROR", f"Expected FAOBJ=ERROR, got {record['faobj']}"


def test_relrec_merge_multiple_relationships_no_duplicates(sdtm_standards_context):
    """Test that multiple relationships for same record don't create duplicates."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    # Create original data with one record
    original_data = {
        "STUDYID": ["STUDY001"],
        "USUBJID": ["SUBJ001"],
        "FASEQ": [1],
        "FAGRPID": ["1"],
        "FADECOD": ["TEST"],
    }
    original_schema = PostgresQLDataService.add_test_dataset(ds, "fa", original_data, sdtm_standards_context)

    # Create right domain data with multiple related records
    right_data = {
        "STUDYID": ["STUDY001", "STUDY001"],
        "USUBJID": ["SUBJ001", "SUBJ001"],
        "CMSEQ": [1, 2],
        "CMGRPID": ["1", "1"],  # Both relate to same FA record
        "FAOBJ": ["VALUE1", "VALUE2"],
    }
    PostgresQLDataService.add_test_dataset(ds, "cm", right_data, sdtm_standards_context)

    # Create RELREC data with multiple relationships to same FA record
    relrec_data = {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001", "STUDY001"],
        "USUBJID": ["SUBJ001", "SUBJ001", "SUBJ001", "SUBJ001"],
        "RELID": ["REL1", "REL1", "REL2", "REL2"],
        "RDOMAIN": ["FA", "CM", "FA", "CM"],
        "IDVAR": ["FAGRPID", "CMGRPID", "FAGRPID", "CMGRPID"],
        "IDVARVAL": ["1", "1", "1", "1"],  # Specific values - should use WHERE filtering
    }
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", relrec_data, sdtm_standards_context)

    # Perform RELREC merge
    result_schema = SqlRelrecMerge.perform_join(
        pgi=pgi,
        original=original_schema,
        relrec=relrec_schema,
        domain="FA",
        wildcard="CM",
    )

    # Count total records in result
    pgi.execute_sql(f"SELECT COUNT(*) as count FROM {result_schema.hash}")
    result = pgi.fetch_all()
    total_count = result[0]["count"]

    # Should have exactly 2 records (one for each CM relationship), not 4
    # The UNION should eliminate any duplicates from multiple RELREC relationships
    assert total_count == 2, f"Expected 2 records (no duplicates), got {total_count}"

    # Verify we have the expected FAOBJ values
    pgi.execute_sql(
        f"""
        SELECT DISTINCT {result_schema.get_column_hash('relrec.faobj')} as faobj
        FROM {result_schema.hash}
        ORDER BY faobj
    """
    )
    faobj_results = pgi.fetch_all()
    faobj_values = [r["faobj"] for r in faobj_results]

    assert len(faobj_values) == 2, f"Expected 2 distinct FAOBJ values, got {len(faobj_values)}"
    assert "VALUE1" in faobj_values, "Should have VALUE1"
    assert "VALUE2" in faobj_values, "Should have VALUE2"
