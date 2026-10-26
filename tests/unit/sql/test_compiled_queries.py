import copy
from types import SimpleNamespace

import pytest
from unittest.mock import patch


from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.models.sql_result_handler import SqlResultHandler
from cdisc_rules_engine.models.test_dataset import TestDataset
from cdisc_rules_engine.sql_rules_engine import SQLRulesEngine
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata
from cdisc_rules_engine.standards.default_standards_context import DefaultStandardsContext
from cdisc_rules_engine.utilities.sql_rule_processor import SQLRuleProcessor

AE = {
    "USUBJID": ["S1", "S1", "S2", "S2", "S3", "S3"],
    "AESEQ": [1, 2, 1, 2, 1, 2],
    "AETERM": ["HEADACHE", "NAUSEA", "HEADACHE", "", None, "NAUSEA"],
    "AEDECOD": ["Headache", None, "Headache", "", "Nausea", "Nausea"],
    "AESTDTC": ["2020-01-01", "2020-02-01", "", None, "2020-03-01", "2020-04-01"],
    "AEENDTC": ["2020-01-02", "2020-01-15", "", "2020-05-01", None, "2020-04-02"],
    "AEGRP": ["A", "A", "B", "B", "C", None],
}


class StandardsContext(DefaultStandardsContext):
    def __init__(self):
        self.standard = "SDTMIG"
        self.standard_version = "3.4"
        self.standard_substandard = None
        self.define_xml_version = None

    def transform_dataset_metadata(self, source):
        return source


def condition(operator, target, comparator=None, **value):
    value = {"target": target, **value}
    if comparator is not None:
        value["comparator"] = comparator
    return {"name": "get_dataset", "operator": operator, "value": value}


def rule(conditions, sensitivity="Record", operations=None, output_variables=None):
    return {
        "core_id": "TEST.COMPILED",
        "classes": {"Include": ["ALL"]},
        "domains": {"Include": ["ALL"]},
        "rule_type": "Record Data",
        "sensitivity": sensitivity,
        "severity": "error",
        "standards": [{"Name": "SDTMIG", "Version": "3.4"}],
        "conditions": conditions,
        "operations": operations or [],
        "output_variables": output_variables or ["AETERM", "AEDECOD"],
        "actions": [{"name": "generate_dataset_error_objects", "params": {"message": "AE check failed"}}],
    }


def run(rule_definition, max_errors=None, split_part_filenames=None):
    data_service = PostgresQLDataService.from_list_of_testdatasets(
        [TestDataset.from_records("ae", AE)], StandardsContext()
    )
    dataset = BaseDatasetMetadata(**{k: v for k, v in data_service.datasets[0].__dict__.items()}, domain="AE")
    if split_part_filenames:
        dataset.split_part_filenames = split_part_filenames
        table = data_service.pgi.schema.get_table("ae")
        per_part = len(AE["USUBJID"]) // len(split_part_filenames)
        for index, filename in enumerate(split_part_filenames):
            data_service.pgi.execute_sql(
                f"UPDATE {table.hash} SET {table.get_column_hash('source_ds')} = '{filename.split('.')[0].upper()}' "
                f"WHERE id > {index * per_part} AND id <= {(index + 1) * per_part}"
            )
    data_service.datasets = [dataset]
    statements = []
    execute_sql = data_service.pgi.execute_sql

    def record(query, *args, **kwargs):
        statements.append(query)
        return execute_sql(query, *args, **kwargs)

    data_service.pgi.execute_sql = record
    engine = (
        SQLRulesEngine(data_service, StandardsContext(), max_errors_per_dataset=max_errors)
        if max_errors
        else SQLRulesEngine(data_service, StandardsContext())
    )
    try:
        containers = next(iter(engine.sql_validate_single_rule(copy.deepcopy(rule_definition)).values()))
        return (containers if split_part_filenames else containers[0]), statements
    finally:
        data_service.pgi.close()


EVERY_ROW = {"all": [condition("non_empty", "USUBJID")]}


def test_a_record_rule_reports_every_failing_row():
    reported, _ = run(rule(EVERY_ROW, output_variables=["AESEQ"]))
    assert len(reported["errors"]) == 6
    assert reported["message"] == "AE check failed"


def test_record_errors_are_capped_and_the_message_says_how_many_there_were():
    reported, _ = run(rule(EVERY_ROW, output_variables=["AESEQ"]), max_errors=2)
    assert [error["row"] for error in reported["errors"]] == [1, 2]
    assert reported["message"] == "AE check failed (2 of 6 errors shown)"


def test_a_split_dataset_is_capped_per_source_file():
    reported, _ = run(
        rule(EVERY_ROW, output_variables=["AESEQ"]), max_errors=2, split_part_filenames=["ae1.xpt", "ae2.xpt"]
    )
    by_file = {container["dataset"]: container for container in reported}
    assert {name: len(container["errors"]) for name, container in by_file.items()} == {"ae1.xpt": 2, "ae2.xpt": 2}
    assert {name: container["message"] for name, container in by_file.items()} == {
        "ae1.xpt": "AE check failed (2 of 3 errors shown)",
        "ae2.xpt": "AE check failed (2 of 3 errors shown)",
    }


def test_a_not_group_is_true_where_its_checks_are_not():
    reported, _ = run(rule({"not": {"all": [condition("non_empty", "AETERM")]}}, output_variables=["AETERM"]))
    assert [error["row"] for error in reported["errors"]] == [4, 5]


def test_a_dataset_rule_reports_its_first_failing_row_once():
    reported, statements = run(rule(EVERY_ROW, sensitivity="Dataset", output_variables=["AESEQ"]))
    assert [error["value"] for error in reported["errors"]] == [{"AESEQ": 1}]
    assert any("LIMIT 1" in statement for statement in statements)


def grouped(*grouping_variables):
    definition = rule(EVERY_ROW, sensitivity="Group", output_variables=["USUBJID", "AESEQ"])
    definition["grouping_variables"] = list(grouping_variables)
    return definition


def test_a_group_rule_reports_the_first_failing_row_of_each_group():
    reported, _ = run(grouped("USUBJID"))
    assert [(error["value"]["USUBJID"], error["row"]) for error in reported["errors"]] == [
        ("S1", 1),
        ("S2", 3),
        ("S3", 5),
    ]


def test_a_grouping_variable_the_dataset_lacks_puts_every_row_in_one_group():
    reported, _ = run(grouped("USUBJID", "--NOTAVARIABLE"))
    assert [error["row"] for error in reported["errors"]] == [1, 3, 5]


def test_filtering_groups_by_a_dataset_name_the_dataset_lacks_reports_nothing():
    reported, _ = run(grouped("filter_by_dataset"))
    assert reported["errors"] == []


def test_a_postgres_error_is_reported_as_an_error_in_the_check_that_caused_it():
    reported, _ = run(rule({"all": [condition("non_empty", "AETERM"), condition("matches_regex", "AETERM", "(")]}))
    assert reported["executionStatus"] == "execution_error"
    assert reported["errors"][0]["error"] == "SQL error in matches_regex operator"


def with_operations(operation_variables):
    return patch.object(SQLRuleProcessor, "perform_rule_operations", return_value=operation_variables)


def test_operation_values_are_selected_with_the_failing_rows():
    operations = {
        "$terms": SqlOperationResult(
            query="SELECT value FROM (VALUES ('HEADACHE'), (NULL)) AS terms(value)", type="collection", subtype="Char"
        ),
        "$seq_terms": SqlOperationResult(
            query="SELECT value FROM (VALUES (1, 'ONE')) AS terms(seq, value) WHERE seq = $1",
            type="collection",
            subtype="Char",
            params={"$1": "AESEQ"},
        ),
    }
    with with_operations(operations):
        reported, statements = run(rule(EVERY_ROW, output_variables=["AESEQ", "$terms", "$seq_terms"]))
    values = [error["value"] for error in reported["errors"][:2]]
    assert values == [
        {"AESEQ": 1, "$terms": ["HEADACHE"], "$seq_terms": ["ONE"]},
        {"AESEQ": 2, "$terms": ["HEADACHE"], "$seq_terms": None},
    ]
    assert sum("ARRAY(" in statement for statement in statements) == 1, "selected in the rule's own query"


def test_an_operation_value_the_query_cannot_select_is_evaluated_per_row():
    operations = {"$pair": SqlOperationResult(query="SELECT 1 AS first, 2 AS second", type="constant", subtype="Num")}
    with with_operations(operations):
        reported, _ = run(rule(EVERY_ROW, output_variables=["$pair"]))
    assert reported["errors"][0]["value"] == {"$pair": 1}


def test_an_operation_value_evaluated_per_row_reads_the_reported_row():
    operations = {
        "$pair": SqlOperationResult(
            query="SELECT $1 AS first, 2 AS second", type="constant", subtype="Num", params={"$1": "AESEQ"}
        )
    }
    with with_operations(operations):
        reported, _ = run(rule(EVERY_ROW, output_variables=["$pair"]))
    assert [error["value"]["$pair"] for error in reported["errors"][:2]] == [1, 2]


def test_an_operator_failing_on_a_missing_value_is_an_error_not_a_pass():
    sorted_by = condition(
        "target_is_sorted_by", "AESEQ", [{"name": "AESTDTC", "sort_order": "ASC", "null_position": "last"}]
    )
    reported, _ = run(rule({"all": [sorted_by]}))
    assert reported["executionStatus"] == "execution_error"
    assert reported["errors"][0]["error"] == "SQL error in target_is_sorted_by operator"


def test_a_condition_that_is_both_all_and_any_is_a_rule_format_error():
    reported, _ = run(rule({"all": [condition("non_empty", "AETERM")], "any": [condition("empty", "AEDECOD")]}))
    assert reported["executionStatus"] == "execution_error"
    assert reported["errors"][0]["error"] == "Rule format error"


def test_a_text_parameter_evaluated_per_row_is_escaped():
    handler = SqlResultHandler.__new__(SqlResultHandler)
    schema = SimpleNamespace(get_column_hash=lambda column: column.lower())
    query = handler._substitute_parameters(
        "SELECT $1 AS a, $10 AS b", {"$1": "TERM", "$10": "OTHER"}, {"term": "O'BRIEN", "other": "X"}, schema
    )
    assert query == "SELECT 'O''BRIEN' AS a, 'X' AS b"


@pytest.mark.parametrize("group", ["all", "any"])
def test_an_empty_all_or_any_is_a_rule_format_error(group):
    reported, _ = run(rule({"all": [condition("non_empty", "AETERM"), {group: []}]}))
    assert reported["executionStatus"] == "execution_error"
    assert reported["errors"][0]["error"] == "Rule format error"


def test_an_unimplemented_operator_is_an_error():
    reported, _ = run(rule({"all": [condition("conformant_value_data_type", "AETERM")]}))
    assert reported["executionStatus"] == "execution_error"
    assert reported["errors"][0]["error"] == "SQL error in conformant_value_data_type operator"
