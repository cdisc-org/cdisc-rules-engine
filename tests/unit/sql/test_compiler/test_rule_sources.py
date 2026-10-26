"""The rows each rule type's checks read, as views over the study's data and metadata."""

from unittest.mock import patch

import pytest

from cdisc_rules_engine.compiler.sources import rule_source
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.metadata.store import MetadataStore
from cdisc_rules_engine.models.test_dataset import TestDataset
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata
from cdisc_rules_engine.standards.default_standards_context import DefaultStandardsContext

LB = {
    "STUDYID": ["S", "S", "S"],
    "DOMAIN": ["LB", "LB", "LB"],
    "USUBJID": ["1", "1", "2"],
    "LBSEQ": [1, 2, 1],
    "LBTESTCD": ["GLUC", "ALB", "GLUC"],
    "LBORRES": ["5", "40", "7"],
}
SUPPLB = {
    "STUDYID": ["S", "S"],
    "RDOMAIN": ["LB", "LB"],
    "USUBJID": ["1", "2"],
    "IDVAR": ["LBSEQ", "LBSEQ"],
    "IDVARVAL": ["1", "1"],
    "QNAM": ["LBKEY", "LBKEY"],
    "QVAL": ["A", "A"],
}


class StandardsContext(DefaultStandardsContext):
    def __init__(self):
        self.standard = "SDTMIG"
        self.standard_version = "3.4"
        self.standard_substandard = None
        self.define_xml_version = None

    def transform_dataset_metadata(self, source):
        return source


@pytest.fixture
def study():
    data_service = PostgresQLDataService.from_list_of_testdatasets(
        [TestDataset.from_records("lb", LB), TestDataset.from_records("supplb", SUPPLB)], StandardsContext()
    )
    data_service.datasets = [
        BaseDatasetMetadata(**{k: v for k, v in dataset.__dict__.items()}, domain=dataset.name.upper())
        for dataset in data_service.datasets
    ]
    yield data_service
    data_service.pgi.close()


STANDARDS_CONTEXT = StandardsContext()


def rows(data_service, rule_type, dataset="lb", **rule):
    metadata = data_service.get_dataset_metadata(dataset)
    table_id = rule_source(
        {"rule_type": rule_type, **rule}, data_service, metadata, STANDARDS_CONTEXT, data_service.datasets
    )
    schema = data_service.pgi.schema.get_table(table_id)
    data_service.pgi.execute_sql(f"SELECT * FROM {schema.hash} ORDER BY id")
    names = {column.hash: name for name, column in schema.get_columns() if not column.alias}
    return [{names.get(key, key): value for key, value in row.items()} for row in data_service.pgi.fetch_all()]


def test_a_record_rule_reads_its_dataset(study):
    assert rule_source({"rule_type": "Record Data"}, study, study.get_dataset_metadata("lb"), None, []) == "lb"


def test_value_rows_follow_the_variables_one_after_the_other(study):
    values = rows(study, "Value Check with Variable Metadata")
    testcd = [row for row in values if row["variable_name"] == "LBTESTCD"]
    # the fifth variable's rows come after the first four variables' three rows each
    assert [(row["id"], row["row_number"], row["variable_value"]) for row in testcd] == [
        (13, 1.0, "GLUC"),
        (14, 2.0, "ALB"),
        (15, 3.0, "GLUC"),
    ]
    assert {row["variable_value_length"] for row in testcd} == {3.0, 4.0}


def test_a_value_level_metadata_describes_the_values_its_where_clause_matches(study):
    vlm = {
        "define_variable_name": "LBORRES",
        "define_vlm_name": "LBORRES.GLUC",
        "define_vlm_data_type": "integer",
        "where_clause": [{"variable": "LBTESTCD", "comparator": "EQ", "values": ["GLUC"]}],
    }
    with patch.object(MetadataStore, "define_vlms", return_value=[vlm]):
        described = rows(study, "Value Check against Define XML VLM")
    assert [(row["variable_value"], row["define_vlm_name"], row["lbseq"]) for row in described] == [
        ("5", "LBORRES.GLUC", 1.0),
        ("7", "LBORRES.GLUC", 1.0),
    ]


def test_library_metadata_missing_from_a_variable_is_the_text_none(study):
    library = [{"library_variable_name": "LBTESTCD", "library_variable_ccode": None, "library_variable_core": "Req"}]
    with patch.object(MetadataStore, "library_variables", return_value=library):
        values = rows(study, "Value Check against Library Metadata")
    testcd = next(row for row in values if row["variable_name"] == "LBTESTCD")
    assert testcd["library_variable_core"] == "Req"
    assert testcd["library_variable_ccode"] == "None"
    assert testcd["library_variable_order_number"] is None


def test_define_keys_are_unique_when_their_values_identify_a_record(study):
    with patch.object(MetadataStore, "define_dataset", return_value={"define_dataset_key_sequence": "USUBJID,QNAM"}):
        records = rows(study, "Dataset Contents Check against Define XML", dataset="supplb")
    assert [row["define_key_sequence_is_unique"] for row in records] == [True, True]


def test_supp_qualifiers_take_part_in_define_keys(study):
    with patch.object(MetadataStore, "define_dataset", return_value={"define_dataset_key_sequence": "STUDYID,LBKEY"}):
        records = rows(study, "Dataset Contents Check against Define XML")
    # LBKEY comes from SUPPLB: both subjects' first records have LBKEY A, the second record has none
    assert [row["define_key_sequence_is_unique"] for row in records] == [False, True, False]
    assert {row["dataset_name"] for row in records} == {"lb"}


def test_domain_presence_has_a_column_per_domain_holding_its_files(study):
    (row,) = rows(study, "Domain Presence Check")
    assert {key: value for key, value in row.items() if key != "id"} == {"lb": "lb.xpt", "supplb": "supplb.xpt"}


def test_metadata_is_read_once_per_study(study):
    with patch.object(MetadataStore, "_read_library_variables", return_value=[]) as read:
        rows(study, "Variable Metadata Check against Library Metadata")
        rows(study, "Value Check against Library Metadata")
    assert read.call_count == 1


def test_a_rules_view_is_never_taken_for_an_uploaded_dataset(study):
    from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
    from cdisc_rules_engine.sql_operations.dataset_names import SqlDatasetNamesOperation

    metadata = study.get_dataset_metadata("lb")
    with patch.object(MetadataStore, "define_dataset", return_value={}):
        view = rule_source(
            {"rule_type": "Dataset Contents Check against Define XML"},
            study,
            metadata,
            STANDARDS_CONTEXT,
            study.datasets,
        )
    assert study.pgi.schema.get_table(view).source == "derived"
    names = SqlDatasetNamesOperation(
        SqlOperationParams(domain="LB", target=None, standards_context=STANDARDS_CONTEXT), study
    ).execute()
    study.pgi.execute_sql(names.query)
    assert {row["value"] for row in study.pgi.fetch_all()} == {"LB", "SUPPLB"}


def test_a_rules_views_are_dropped_with_the_studys_tables(study):
    rows(study, "Variable Metadata Check")  # literal rows: a view that depends on no table
    study.pgi.execute_sql("SELECT COUNT(*) AS n FROM pg_views WHERE schemaname = 'public'")
    assert study.pgi.fetch_one()["n"] > 0
    study.pgi._drop_prefixed_tables()
    study.pgi.execute_sql(
        f"SELECT COUNT(*) AS n FROM pg_views WHERE schemaname = 'public' AND viewname LIKE '{study.pgi.sql_namespace}%'"
    )
    assert study.pgi.fetch_one()["n"] == 0
