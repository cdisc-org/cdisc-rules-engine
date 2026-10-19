import copy
import pytest

from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.test_dataset import TestDataset
from cdisc_rules_engine.sql_rules_engine import SQLRulesEngine
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata
from cdisc_rules_engine.standards.default_standards_context import DefaultStandardsContext

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


RULES = {
    "all_conditions": rule(
        {"all": [condition("equal_to", "AETERM", "HEADACHE", value_is_literal=True), condition("non_empty", "AEDECOD")]}
    ),
    "any_conditions": rule(
        {"any": [condition("equal_to", "AETERM", "NAUSEA", value_is_literal=True), condition("empty", "AESTDTC")]}
    ),
    "any_with_a_missing_variable_only_drops_that_branch": rule(
        {
            "any": [
                condition("equal_to", "NOTAVARIABLE", "X", value_is_literal=True),
                condition("equal_to", "AETERM", "NAUSEA", value_is_literal=True),
            ]
        }
    ),
    "all_with_a_missing_variable": rule(
        {
            "all": [
                condition("equal_to", "NOTAVARIABLE", "X", value_is_literal=True),
                condition("equal_to", "AETERM", "NAUSEA", value_is_literal=True),
            ]
        }
    ),
    "negated_operators_over_nulls": rule({"all": [condition("non_empty", "AETERM"), condition("not_exists", "AEXXX")]}),
    "nested": rule(
        {
            "all": [
                condition("exists", "AETERM"),
                {
                    "any": [
                        {"all": [condition("empty", "AEDECOD"), condition("non_empty", "AETERM")]},
                        condition("equal_to", "AETERM", "HEADACHE", value_is_literal=True),
                    ]
                },
            ]
        }
    ),
    "a_not_group": rule(
        {"not": {"all": [condition("non_empty", "AETERM"), condition("non_empty", "AEDECOD")]}},
    ),
    "dataset_sensitivity_reports_the_first_failing_row": rule(
        {"all": [condition("empty", "AEDECOD")]}, sensitivity="Dataset", output_variables=["AEDECOD"]
    ),
    "against_an_operation": rule(
        {"all": [condition("less_than", "AESEQ", "$maxseq")]},
        operations=[{"id": "$maxseq", "name": "AESEQ", "operator": "max"}],
        output_variables=["AESEQ"],
    ),
    "grouped_max": rule(
        {"all": [condition("less_than", "AESEQ", "$maxseq")]},
        operations=[{"id": "$maxseq", "name": "AESEQ", "operator": "max", "group": ["USUBJID"]}],
        output_variables=["AESEQ"],
    ),
    "grouped_count_over_a_group_with_a_null_key": rule(
        {"all": [condition("equal_to", "AESEQ", "$rows")]},
        operations=[{"id": "$rows", "operator": "record_count", "group": ["AEGRP"]}],
        output_variables=["AEGRP", "AESEQ"],
    ),
    "grouped_count_where_a_group_has_nothing_to_count": rule(
        {"all": [condition("greater_than", "AESEQ", "$nausea")]},
        operations=[{"id": "$nausea", "operator": "record_count", "group": ["AEGRP"], "filter": {"AETERM": "NAUSEA"}}],
        output_variables=["AEGRP", "AESEQ"],
    ),
    "grouped_count_where_empty_groups_are_dropped": rule(
        {"all": [condition("greater_than", "AESEQ", "$nausea")]},
        operations=[
            {
                "id": "$nausea",
                "operator": "record_count",
                "group": ["AEGRP"],
                "filter": {"AETERM": "NAUSEA"},
                "ignore_empty_filtered_groups": True,
            }
        ],
        output_variables=["AEGRP", "AESEQ"],
    ),
    "grouped_max_date": rule(
        {"all": [condition("date_greater_than", "AEENDTC", "$last_start")]},
        operations=[{"id": "$last_start", "name": "AESTDTC", "operator": "max_date", "group": ["USUBJID"]}],
        output_variables=["AESTDTC", "AEENDTC"],
    ),
    "grouped_operation_by_a_variable_the_dataset_lacks": rule(
        {"all": [condition("less_than", "AESEQ", "$maxseq")]},
        operations=[{"id": "$maxseq", "name": "AESEQ", "operator": "max", "group": ["USUBJID"]}],
        output_variables=["AESEQ"],
    ),
    "values_that_are_not_a_unique_set": rule(
        {"all": [condition("is_not_unique_set", "AETERM")]}, output_variables=["AETERM"]
    ),
    "values_that_are_a_unique_set_across_columns": rule(
        {"all": [condition("is_unique_set", "USUBJID", ["AESEQ"])]}, output_variables=["USUBJID"]
    ),
    "a_relationship_that_is_not_unique": rule(
        {"all": [condition("is_not_unique_relationship", "AETERM", "AEDECOD")]}, output_variables=["AETERM", "AEDECOD"]
    ),
    "a_relationship_across_several_comparators": rule(
        {"all": [condition("is_not_unique_relationship", "USUBJID", ["AEGRP", "AEDECOD"])]},
        output_variables=["USUBJID", "AEGRP"],
    ),
    "values_inconsistent_within_a_group": rule(
        {"all": [condition("is_inconsistent_across_dataset", "AESTDTC", "AETERM")]},
        output_variables=["AETERM", "AESTDTC"],
    ),
    "values_inconsistent_within_a_group_with_a_null_key": rule(
        {"all": [condition("is_inconsistent_across_dataset", "AETERM", "AEGRP")]}, output_variables=["AEGRP", "AETERM"]
    ),
    "values_inconsistent_within_groups_of_several_columns": rule(
        {"all": [condition("is_inconsistent_across_dataset", "AEENDTC", ["AETERM", "AEDECOD"])]},
        output_variables=["AETERM", "AEDECOD", "AEENDTC"],
    ),
    "values_inconsistent_where_populated": rule(
        {"all": [condition("is_inconsistent_across_dataset", "AEENDTC", "AEGRP", where_populated=True)]},
        output_variables=["AEGRP", "AEENDTC"],
    ),
    "values_inconsistent_where_other_columns_are_populated": rule(
        {"all": [condition("is_inconsistent_across_dataset", "AEENDTC", "AEGRP", where_populated_columns=["AESTDTC"])]},
        output_variables=["AEGRP", "AEENDTC"],
    ),
    "date_comparison": rule({"all": [condition("date_less_than", "AEENDTC", "AESTDTC")]}),
    "no_row_fails": rule({"all": [condition("equal_to", "AETERM", "NOT_IN_THE_DATA", value_is_literal=True)]}),
}


def run(rule_definition, compile_rules):
    """Returns what the rule reported, whether each attempt to compile it succeeded, and the SQL it ran."""
    data_service = PostgresQLDataService.from_list_of_testdatasets(
        [TestDataset.from_records("ae", AE)], StandardsContext()
    )
    data_service.datasets = [
        BaseDatasetMetadata(**{k: v for k, v in data_service.datasets[0].__dict__.items()}, domain="AE")
    ]
    engine = SQLRulesEngine(data_service, StandardsContext(), compile_rules=compile_rules)

    statements = []
    execute_sql = data_service.pgi.execute_sql

    def record_statement(query, *args, **kwargs):
        statements.append(query)
        return execute_sql(query, *args, **kwargs)

    data_service.pgi.execute_sql = record_statement

    compiled_attempts = []
    run_compiled = engine._run_compiled

    def record(*args, **kwargs):
        compiled_attempts.append(run_compiled(*args, **kwargs))
        return compiled_attempts[-1]

    engine._run_compiled = record
    try:
        results = engine.sql_validate_single_rule(copy.deepcopy(rule_definition))
        return results, compiled_attempts, statements
    finally:
        data_service.pgi.close()


@pytest.mark.parametrize("name", list(RULES))
def test_compiled_rule_reports_what_the_row_by_row_rule_does(name):
    row_by_row, row_by_row_attempts, _ = run(RULES[name], compile_rules=False)
    compiled, compiled_attempts, _ = run(RULES[name], compile_rules=True)

    assert row_by_row_attempts == []
    assert compiled_attempts and all(compiled_attempts), "the rule fell back to running row by row"
    assert compiled == row_by_row


def test_these_rules_actually_fail_somewhere():
    reported = {
        name: bool(next(iter(run(RULES[name], compile_rules=False)[0].values()))[0].get("errors")) for name in RULES
    }
    assert reported["all_conditions"] and reported["any_conditions"] and reported["nested"]
    assert reported["any_with_a_missing_variable_only_drops_that_branch"]
    assert not reported["no_row_fails"]


GROUPED_RULES = [name for name in RULES if name.startswith("grouped_") and "lacks" not in name]


@pytest.mark.parametrize("name", GROUPED_RULES)
def test_grouped_operations_are_joined_rather_than_asked_for_row_by_row(name):
    _, _, statements = run(RULES[name], compile_rules=True)
    _, _, row_by_row_statements = run(RULES[name], compile_rules=False)

    assert any("LEFT JOIN" in statement for statement in statements)
    assert not any("LEFT JOIN" in statement for statement in row_by_row_statements)


@pytest.mark.parametrize("name", GROUPED_RULES)
def test_grouped_rules_fail_somewhere(name):
    """Guards the comparison against passing only because the rule never reports anything."""
    results, _, _ = run(RULES[name], compile_rules=False)
    assert next(iter(results.values()))[0].get("errors")


COLUMN_FREE = [name for name in RULES if name.startswith(("values_that_", "a_relationship", "values_inconsistent"))]


@pytest.mark.parametrize("name", COLUMN_FREE)
def test_uniqueness_checks_no_longer_rewrite_the_table(name):
    _, _, statements = run(RULES[name], compile_rules=True)
    _, _, row_by_row_statements = run(RULES[name], compile_rules=False)

    assert not any(statement.lstrip().upper().startswith("UPDATE") for statement in statements)
    assert any(statement.lstrip().upper().startswith("UPDATE") for statement in row_by_row_statements)
