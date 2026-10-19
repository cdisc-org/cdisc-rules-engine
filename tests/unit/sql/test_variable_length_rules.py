import copy

import pytest

from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.test_dataset import TestDataset
from cdisc_rules_engine.sql_rules_engine import SQLRulesEngine
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata
from cdisc_rules_engine.standards.default_standards_context import DefaultStandardsContext

DATASETS = {
    "SUPPAE": (
        {"USUBJID": ["001", "002"], "QNAM": ["AESER", "AESER"], "QVAL": ["Y", "NO"]},
        {"USUBJID": 20, "QNAM": 8, "QVAL": 2},
    ),
    "SUPPCM": (
        {"USUBJID": ["001"], "QNAM": ["CMLONGNAME1"], "QVAL": [None]},
        {"USUBJID": 20, "QNAM": 8, "QVAL": 4},
    ),
    "SUPPEX": ({"USUBJID": [], "QNAM": [], "QVAL": []}, {"USUBJID": 200, "QNAM": 200, "QVAL": 200}),
}


class StandardsContext(DefaultStandardsContext):
    def __init__(self):
        self.standard = "SDTMIG"
        self.standard_version = "3.4"
        self.standard_substandard = None
        self.define_xml_version = None

    def transform_dataset_metadata(self, source):
        return source


def datasets():
    for name, (records, declared_lengths) in DATASETS.items():
        dataset = TestDataset.from_records(name, records)
        for variable in dataset.variables:
            variable.length = declared_lengths[variable.name]
            variable.type = "Char"
        yield dataset


def base_rule(rule_type, conditions, operations, output_variables, grouping_variables):
    return {
        "core_id": "TEST.LENGTHS",
        "classes": {"Include": ["ALL"]},
        "domains": {"Include": ["ALL"]},
        "rule_type": rule_type,
        "sensitivity": "Group",
        "grouping_variables": grouping_variables,
        "severity": "error",
        "standards": [{"Name": "SDTMIG", "Version": "3.4"}],
        "conditions": conditions,
        "operations": operations,
        "output_variables": output_variables,
        "actions": [{"name": "generate_dataset_error_objects", "params": {"message": "Length check failed"}}],
    }


def condition(operator, target, comparator):
    return {"name": "get_dataset", "operator": operator, "value": {"target": target, "comparator": comparator}}


MAX_LENGTH_OF_VALUES = {"id": "$max_variable_value_length"}
GLOBAL_OPERATIONS = [
    {**MAX_LENGTH_OF_VALUES},
    {"id": "$global_max_variable_length"},
    {"id": "$global_min_variable_length"},
]

DECLARED_LONGER_THAN_DATA = (
    base_rule(
        "Value Check with Variable Metadata",
        {"all": [condition("greater_than", "variable_length", "$max_variable_value_length")]},
        [
            {
                **MAX_LENGTH_OF_VALUES,
                "name": "variable_value_length",
                "operator": "max",
                "group": ["variable_name"],
                "use_rule_type_table": True,
            }
        ],
        ["variable_name", "$max_variable_value_length", "variable_length"],
        ["variable_name"],
    ),
    base_rule(
        "Value Check with Variable Metadata",
        {"all": [condition("greater_than", "variable_length", "$max_variable_value_length")]},
        [{**MAX_LENGTH_OF_VALUES, "name": "variable_name", "operator": "max_variable_value_length"}],
        ["variable_name", "$max_variable_value_length", "variable_length"],
        ["variable_name"],
    ),
)

GLOBAL_CHECK = {
    "any": [
        condition("not_equal_to", "variable_length", "$max_variable_value_length"),
        condition("not_equal_to", "$global_max_variable_length", "$global_min_variable_length"),
        condition("not_equal_to", "variable_length", "$global_max_variable_length"),
        condition("not_equal_to", "variable_length", "$global_min_variable_length"),
    ]
}
GLOBAL_OUTPUT = [
    "variable_name",
    "$max_variable_value_length",
    "$global_min_variable_length",
    "$global_max_variable_length",
]
GLOBAL_GROUPING = ["variable_name", "dataset_name", "filter_by_dataset"]
DIFFERENT_LENGTHS_ACROSS_DATASETS = (
    base_rule(
        "Global Value Check with Variable Metadata",
        GLOBAL_CHECK,
        [
            {
                "id": "$max_variable_value_length",
                "domain": "global_value_check",
                "name": "variable_value_length",
                "operator": "max",
                "group": ["variable_name"],
            },
            {
                "id": "$global_max_variable_length",
                "domain": "global_value_check",
                "name": "variable_length",
                "operator": "max",
                "group": ["variable_name"],
            },
            {
                "id": "$global_min_variable_length",
                "domain": "global_value_check",
                "name": "variable_length",
                "operator": "min",
                "group": ["variable_name"],
            },
        ],
        GLOBAL_OUTPUT,
        GLOBAL_GROUPING,
    ),
    base_rule(
        "Global Value Check with Variable Metadata",
        GLOBAL_CHECK,
        [
            {"id": "$max_variable_value_length", "name": "variable_name", "operator": "max_variable_value_length"},
            {"id": "$global_max_variable_length", "name": "variable_name", "operator": "global_max_variable_length"},
            {"id": "$global_min_variable_length", "name": "variable_name", "operator": "global_min_variable_length"},
        ],
        GLOBAL_OUTPUT,
        GLOBAL_GROUPING,
    ),
)


AS_GLOBAL_VARIABLE_METADATA_CHECK = {
    **DIFFERENT_LENGTHS_ACROSS_DATASETS[1],
    "rule_type": "Global Variable Metadata Check",
}


def findings(rule, compile_rules):
    data_service = PostgresQLDataService.from_list_of_testdatasets(list(datasets()), StandardsContext())
    data_service.datasets = [
        BaseDatasetMetadata(**{k: v for k, v in dataset.__dict__.items()}, domain=dataset.name.upper())
        for dataset in data_service.datasets
    ]
    statements = []
    execute_sql = data_service.pgi.execute_sql

    def record(query, *args, **kwargs):
        statements.append(query)
        return execute_sql(query, *args, **kwargs)

    data_service.pgi.execute_sql = record
    engine = SQLRulesEngine(data_service, StandardsContext(), compile_rules=compile_rules)
    try:
        results = engine.sql_validate_single_rule(copy.deepcopy(rule))
    finally:
        data_service.pgi.close()

    reported = {}
    for dataset, containers in results.items():
        reported[dataset] = sorted(
            tuple(sorted((key, value) for key, value in error["value"].items()))
            for c in containers
            for error in c.get("errors", [])
        )
    return reported, statements


@pytest.mark.parametrize("compile_rules", [False, True], ids=["rows", "compiled"])
@pytest.mark.parametrize(
    "written_with_grouped_operations, written_with_variable_length_operations",
    [DECLARED_LONGER_THAN_DATA, DIFFERENT_LENGTHS_ACROSS_DATASETS],
    ids=["declared_longer_than_data", "different_lengths_across_datasets"],
)
def test_the_operations_find_what_the_grouped_operations_do(
    written_with_grouped_operations, written_with_variable_length_operations, compile_rules
):
    before, _ = findings(written_with_grouped_operations, compile_rules)
    after, _ = findings(written_with_variable_length_operations, compile_rules)

    assert any(before.values()), "the rule finds nothing, so this proves nothing"
    assert after == before


@pytest.mark.parametrize(
    "rule", [DECLARED_LONGER_THAN_DATA[1], DIFFERENT_LENGTHS_ACROSS_DATASETS[1]], ids=["per_dataset", "global"]
)
def test_the_values_of_every_row_are_never_unpivoted(rule):
    _, statements = findings(rule, compile_rules=True)

    assert not any("ROW_NUMBER() OVER" in statement for statement in statements)


@pytest.mark.parametrize("compile_rules", [False, True], ids=["rows", "compiled"])
def test_a_global_variable_metadata_check_finds_what_the_global_value_check_does(compile_rules):
    before, _ = findings(DIFFERENT_LENGTHS_ACROSS_DATASETS[0], compile_rules)
    after, statements = findings(AS_GLOBAL_VARIABLE_METADATA_CHECK, compile_rules)

    assert any(before.values())
    assert after == before
    assert not any("ROW_NUMBER() OVER" in statement for statement in statements)
