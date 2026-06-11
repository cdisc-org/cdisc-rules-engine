"""
Unit tests for SqlDataPreprocessor split dataset functionality.
"""

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.data_service.sql_data_preprocessor import SqlDataPreprocessor

SIMPLE_SPLIT_AE_DATA = {
    "ae1": {
        "studyid": ["ABC", "ABC"],
        "usubjid": ["001", "002"],
        "aeseq": [1, 2],
        "aeterm": ["Headache", "Nausea"],
    },
    "ae2": {
        "studyid": ["ABC", "ABC"],
        "usubjid": ["003", "004"],
        "aeseq": [1, 2],
        "aeterm": ["Fatigue", "Dizziness"],
    },
    "ae3": {
        "studyid": ["ABC"],
        "usubjid": ["005"],
        "aeseq": [1],
        "aeterm": ["Rash"],
    },
}

SPLIT_SUPP_DATA = {
    "suppae1": {
        "studyid": ["ABC", "ABC"],
        "rdomain": ["AE", "AE"],
        "usubjid": ["001", "002"],
        "idvar": ["AESEQ", "AESEQ"],
        "idvarval": ["1", "1"],
        "qnam": ["AESPID", "AESPID"],
        "qval": ["SCREENING", "BASELINE"],
    },
    "suppae2": {
        "studyid": ["ABC"],
        "rdomain": ["AE"],
        "usubjid": ["003"],
        "idvar": ["AESEQ"],
        "idvarval": ["1"],
        "qnam": ["AESPID"],
        "qval": ["TREATMENT"],
    },
}

NON_SPLIT_DATA = {
    "dm": {
        "studyid": ["ABC", "ABC", "ABC"],
        "usubjid": ["001", "002", "003"],
        "age": [25, 30, 35],
        "sex": ["M", "F", "M"],
    },
    "ex": {
        "studyid": ["ABC", "ABC"],
        "usubjid": ["001", "002"],
        "exseq": [1, 1],
        "extrt": ["DRUG A", "DRUG B"],
    },
}


def _get_table_hash(data_service, table_name: str) -> str:
    table_hash = data_service.pgi.schema.get_table_hash(table_name)
    return table_hash if table_hash else table_name.lower()


# ============================================================================
# Concatenation Tests
# ============================================================================


def test_concatenate_two_parts(sdtm_standards_context):
    """Test basic concatenation of two dataset parts."""
    data_service = PostgresQLDataService.instance()
    standards_context = sdtm_standards_context

    PostgresQLDataService.add_test_dataset(
        data_service,
        "ae1",
        {
            "studyid": ["ABC", "ABC"],
            "usubjid": ["001", "002"],
            "aeterm": ["Headache", "Nausea"],
        },
        standards_context,
    )

    PostgresQLDataService.add_test_dataset(
        data_service,
        "ae2",
        {
            "studyid": ["ABC"],
            "usubjid": ["003"],
            "aeterm": ["Fatigue"],
        },
        standards_context,
    )

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    preprocessor._concatenate_split_parts("ae", ["ae1", "ae2"])

    ae_hash = _get_table_hash(data_service, "ae")

    check_query = f"""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = '{ae_hash}'
        )
    """
    data_service.pgi.execute_sql(check_query)
    assert data_service.pgi.fetch_one()["exists"] is True

    data_service.pgi.execute_sql(f"SELECT COUNT(*) as count FROM {ae_hash}")
    assert data_service.pgi.fetch_one()["count"] == 3


def test_concatenate_preserves_source_ds(sdtm_standards_context):
    """Test that SOURCE_DS column values match original table names."""
    data_service = PostgresQLDataService.instance()
    standards_context = sdtm_standards_context

    PostgresQLDataService.add_test_dataset(
        data_service, "ae1", {"studyid": ["ABC"], "usubjid": ["001"]}, standards_context
    )
    PostgresQLDataService.add_test_dataset(
        data_service, "ae2", {"studyid": ["ABC"], "usubjid": ["002"]}, standards_context
    )

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    preprocessor._concatenate_split_parts("ae", ["ae1", "ae2"])

    ae_hash = _get_table_hash(data_service, "ae")

    query = f"SELECT DISTINCT source_ds FROM {ae_hash} ORDER BY source_ds"
    data_service.pgi.execute_sql(query)
    sources = {row["source_ds"] for row in data_service.pgi.fetch_all()}

    assert sources == {"AE1", "AE2"}


def test_concatenate_maintains_source_row_order(sdtm_standards_context):
    """Test that rows are ordered by SOURCE_DS then SOURCE_ROW_NUMBER."""
    data_service = PostgresQLDataService.instance()
    standards_context = sdtm_standards_context

    PostgresQLDataService.add_test_dataset(
        data_service, "ae2", {"studyid": ["ABC"], "usubjid": ["003"]}, standards_context
    )
    PostgresQLDataService.add_test_dataset(
        data_service, "ae1", {"studyid": ["ABC"], "usubjid": ["001"]}, standards_context
    )

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    preprocessor._concatenate_split_parts("ae", ["ae1", "ae2"])

    ae_hash = _get_table_hash(data_service, "ae")

    query = f"SELECT source_ds, source_row_number FROM {ae_hash} ORDER BY source_ds, source_row_number"
    data_service.pgi.execute_sql(query)
    results = data_service.pgi.fetch_all()

    assert results[0]["source_ds"] == "AE1"
    assert results[1]["source_ds"] == "AE2"


def test_concatenate_empty_part(sdtm_standards_context):
    """Test concatenation when one part is empty."""
    data_service = PostgresQLDataService.instance()
    standards_context = sdtm_standards_context

    PostgresQLDataService.add_test_dataset(
        data_service, "ae1", {"studyid": ["ABC"], "usubjid": ["001"]}, standards_context
    )

    PostgresQLDataService.add_test_dataset(
        data_service, "ae2", {"studyid": ["ABC"], "usubjid": ["002"]}, standards_context
    )

    ae2_hash = _get_table_hash(data_service, "ae2")
    data_service.pgi.execute_sql(f"DELETE FROM {ae2_hash}")

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    preprocessor._concatenate_split_parts("ae", ["ae1", "ae2"])

    ae_hash = _get_table_hash(data_service, "ae")
    data_service.pgi.execute_sql(f"SELECT COUNT(*) as count FROM {ae_hash}")
    assert data_service.pgi.fetch_one()["count"] == 1


def test_concatenate_creates_indexes(sdtm_standards_context):
    """Test that appropriate indexes are created on concatenated table."""
    data_service = PostgresQLDataService.instance()
    standards_context = sdtm_standards_context

    PostgresQLDataService.add_test_dataset(
        data_service, "dm1", {"studyid": ["ABC"], "usubjid": ["001"], "age": [25]}, standards_context
    )
    PostgresQLDataService.add_test_dataset(
        data_service, "dm2", {"studyid": ["ABC"], "usubjid": ["002"], "age": [30]}, standards_context
    )

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    preprocessor._concatenate_split_parts("dm", ["dm1", "dm2"])

    dm_hash = _get_table_hash(data_service, "dm")

    index_query = f"""
        SELECT indexname FROM pg_indexes
        WHERE tablename = '{dm_hash}'
        AND schemaname = 'public'
    """
    data_service.pgi.execute_sql(index_query)
    indexes = {row["indexname"] for row in data_service.pgi.fetch_all()}

    assert "idx_dm_source_ds" in indexes
    assert "idx_dm_source_row" in indexes
    assert "idx_dm_studyid_usubjid" in indexes


# ============================================================================
# Metadata Creation Tests
# ============================================================================


def test_create_metadata_basic(sdtm_standards_context):
    """Test basic metadata creation from split parts."""
    data_service = PostgresQLDataService.instance()
    standards_context = sdtm_standards_context

    PostgresQLDataService.add_test_dataset(
        data_service, "ae1", {"studyid": ["ABC"], "usubjid": ["001"], "aeterm": ["Headache"]}, standards_context
    )
    PostgresQLDataService.add_test_dataset(
        data_service, "ae2", {"studyid": ["ABC"], "usubjid": ["002"], "aeterm": ["Nausea"]}, standards_context
    )

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    metadata = preprocessor._create_metadata_from_split_parts("ae", ["ae1", "ae2"])

    assert metadata is not None
    assert metadata.name == "AE"
    assert metadata.filename == "ae.xpt"


def test_create_metadata_merges_variables(sdtm_standards_context):
    """Test that variables from all parts are included in merged metadata."""
    data_service = PostgresQLDataService.instance()
    standards_context = sdtm_standards_context

    PostgresQLDataService.add_test_dataset(
        data_service, "ae1", {"studyid": ["ABC"], "usubjid": ["001"], "aeterm": ["Headache"]}, standards_context
    )

    PostgresQLDataService.add_test_dataset(
        data_service, "ae2", {"studyid": ["ABC"], "usubjid": ["002"], "aedecod": ["NAUSEA"]}, standards_context
    )

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    metadata = preprocessor._create_metadata_from_split_parts("ae", ["ae1", "ae2"])

    var_names = {var.name.upper() for var in metadata.variables}

    assert "STUDYID" in var_names
    assert "USUBJID" in var_names
    assert "AETERM" in var_names or "AEDECOD" in var_names


def test_create_metadata_handles_missing_part(sdtm_standards_context):
    """Test metadata creation when a referenced part doesn't exist."""
    data_service = PostgresQLDataService.instance()
    standards_context = sdtm_standards_context

    PostgresQLDataService.add_test_dataset(
        data_service, "ae1", {"studyid": ["ABC"], "usubjid": ["001"]}, standards_context
    )

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    metadata = preprocessor._create_metadata_from_split_parts("ae", ["ae1", "ae2"])

    assert metadata is not None
    assert metadata.name == "AE"


def test_create_metadata_no_parts_found(sdtm_standards_context):
    """Test metadata creation when no parts have metadata."""
    data_service = PostgresQLDataService.instance()
    standards_context = sdtm_standards_context

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    metadata = preprocessor._create_metadata_from_split_parts("ae", ["ae1", "ae2"])

    assert metadata is None


# ============================================================================
# Integration Tests
# ============================================================================


def test_process_split_datasets_end_to_end(sdtm_standards_context):
    """Test complete flow: detect, concatenate, create metadata."""
    data_service = PostgresQLDataService.instance()
    standards_context = sdtm_standards_context

    for table_name, data in SIMPLE_SPLIT_AE_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, standards_context)

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    preprocessor._process_split_datasets()

    ae_hash = _get_table_hash(data_service, "ae")

    check_table_query = f"""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = '{ae_hash}'
        )
    """
    data_service.pgi.execute_sql(check_table_query)
    assert data_service.pgi.fetch_one()["exists"] is True

    data_service.pgi.execute_sql(f"SELECT COUNT(*) as count FROM {ae_hash}")
    assert data_service.pgi.fetch_one()["count"] == 5


def test_process_multiple_groups(sdtm_standards_context):
    """Test processing multiple split groups in one run."""
    data_service = PostgresQLDataService.instance()
    standards_context = sdtm_standards_context

    all_data = {**SIMPLE_SPLIT_AE_DATA, **SPLIT_SUPP_DATA}

    for table_name, data in all_data.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, standards_context)

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    preprocessor._process_split_datasets()

    for table in ["ae", "suppae"]:
        table_hash = _get_table_hash(data_service, table)
        check_query = f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = '{table_hash}'
            )
        """
        data_service.pgi.execute_sql(check_query)
        exists = data_service.pgi.fetch_one()["exists"]
        assert exists is True, f"Table {table} should exist"


def test_process_no_splits(sdtm_standards_context):
    """Test processing when no split datasets exist."""
    data_service = PostgresQLDataService.instance()
    standards_context = sdtm_standards_context

    for table_name, data in NON_SPLIT_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, standards_context)

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    preprocessor._process_split_datasets()


def test_query_concatenated_dataset(sdtm_standards_context):
    """Test querying a concatenated dataset."""
    data_service = PostgresQLDataService.instance()
    standards_context = sdtm_standards_context

    for table_name, data in SIMPLE_SPLIT_AE_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, standards_context)

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    preprocessor._process_split_datasets()

    ae_hash = _get_table_hash(data_service, "ae")

    query = f"""
        SELECT usubjid, aeterm, source_ds
        FROM {ae_hash}
        WHERE usubjid = '001'
    """
    data_service.pgi.execute_sql(query)
    results = data_service.pgi.fetch_all()

    assert len(results) > 0
    result = results[0]
    assert result["usubjid"] == "001"
    assert result["aeterm"] == "Headache"
    assert result["source_ds"] == "AE1"


def test_filter_by_source_ds(sdtm_standards_context):
    """Test filtering concatenated dataset by SOURCE_DS."""
    data_service = PostgresQLDataService.instance()
    standards_context = sdtm_standards_context

    for table_name, data in SIMPLE_SPLIT_AE_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, standards_context)

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    preprocessor._process_split_datasets()

    ae_hash = _get_table_hash(data_service, "ae")

    query = f"""
        SELECT COUNT(*) as count
        FROM {ae_hash}
        WHERE source_ds = 'AE1'
    """
    data_service.pgi.execute_sql(query)
    result = data_service.pgi.fetch_one()

    expected = len(SIMPLE_SPLIT_AE_DATA["ae1"]["usubjid"])
    assert result["count"] == expected
