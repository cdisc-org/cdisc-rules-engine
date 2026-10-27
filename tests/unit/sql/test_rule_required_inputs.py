from unittest.mock import MagicMock

import pytest

from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.sql_rules_engine import SQLRulesEngine
from cdisc_rules_engine.utilities.rule_required_inputs import (
    DEFINE_XML,
    STF_FILE,
    get_missing_inputs,
    get_missing_inputs_summary,
    get_not_applicable_reason,
    get_required_inputs,
    get_supplied_inputs,
    is_not_applicable,
)
from scripts import run_sql_validation as rsv


def _rule(rule_type="Record Data", conditions=None, operations=None):
    return {
        "core_id": "CORE-TEST",
        "rule_type": rule_type,
        "conditions": conditions
        or {"all": [{"name": "get_dataset", "operator": "non_empty", "value": {"target": "AETERM"}}]},
        "operations": operations,
    }


def _condition(operator, target, comparator=None):
    return {
        "all": [{"name": "get_dataset", "operator": operator, "value": {"target": target, "comparator": comparator}}]
    }


@pytest.mark.parametrize(
    "rule, expected",
    [
        (_rule(), set()),
        (_rule(rule_type="Variable Metadata Check against Define XML"), {DEFINE_XML}),
        (_rule(operations=[{"id": "$x", "operator": "define_variable_metadata"}]), {DEFINE_XML}),
        (_rule(conditions=_condition("equal_to", "define_variable_origin_type", "Derived")), {DEFINE_XML}),
        (_rule(conditions=_condition("is_not_valid_meddra_code_reference", "--HLTCD")), {"meddra"}),
        (
            _rule(
                operations=[
                    {"id": "$v", "operator": "get_external_dictionary_version", "external_dictionary_type": "loinc"}
                ]
            ),
            {"loinc"},
        ),
        (
            _rule(
                operations=[
                    {
                        "id": "$d",
                        "operator": "get_define_external_dictionary_version",
                        "external_dictionary_type": "loinc",
                    }
                ]
            ),
            {DEFINE_XML},
        ),
        # compares against the engine's built-in MedDRA release dates, not the loaded dictionary
        (
            _rule(
                conditions={
                    "all": [
                        {
                            "name": "get_dataset",
                            "operator": "is_not_latest_available_external_dictionary_version",
                            "value": {"target": "TSVAL", "version": "$v", "external_dictionary_type": "meddra"},
                        }
                    ]
                }
            ),
            set(),
        ),
        (_rule(rule_type="STF Metadata Check"), {STF_FILE}),
        # literal values never imply an input
        (_rule(conditions=_condition("equal_to", "TSPARMCD", "UNII")), set()),
        (_rule(conditions=_condition("equal_to", "DSDECOD", "define")), set()),
        # rules that check whether a define.xml was supplied (e.g. CORE-002084) must still run without one
        (_rule(conditions=_condition("not_exists", "define.xml")), set()),
        (_rule(conditions=_condition("exists", "define.xml")), set()),
    ],
)
def test_get_required_inputs(rule, expected):
    assert get_required_inputs(rule) == expected


def test_get_missing_inputs_ignores_supplied_inputs():
    rule = _rule(
        rule_type="Define XML check",
        operations=[{"id": "$v", "operator": "get_external_dictionary_version", "external_dictionary_type": "meddra"}],
    )
    assert get_missing_inputs(rule, set()) == [DEFINE_XML, "meddra"]
    assert get_missing_inputs(rule, get_supplied_inputs("define.xml", None, [])) == ["meddra"]
    assert get_missing_inputs(rule, get_supplied_inputs("define.xml", None, ["meddra"])) == []


def test_get_not_applicable_reason():
    assert get_not_applicable_reason([DEFINE_XML]) == "Not applicable: requires a define.xml (-dxp)"
    assert get_not_applicable_reason([DEFINE_XML, "meddra"]) == (
        "Not applicable: requires a define.xml (-dxp) and the MedDRA dictionary (--meddra)"
    )


def test_get_missing_inputs_summary():
    assert get_missing_inputs_summary({}) == ""
    assert get_missing_inputs_summary({"CORE-1": [STF_FILE]}) == (
        "1 rule was not applicable because an input was not supplied: 1 rule requires a study tagging file (-stf)"
    )
    assert get_missing_inputs_summary({"CORE-1": [DEFINE_XML], "CORE-2": [DEFINE_XML, "meddra"]}) == (
        "2 rules were not applicable because an input was not supplied: "
        "2 rules require a define.xml (-dxp), 1 rule requires the MedDRA dictionary (--meddra)"
    )


def _engine(scope):
    """An engine over datasets AE and DM, where scope maps dataset name -> in rule scope."""
    datasets = []
    for name in scope:
        metadata = MagicMock(filename=f"{name.lower()}.xpt", domain=name, split_part_filenames=None)
        metadata.name = name
        datasets.append(metadata)
    by_name = {metadata.name: metadata for metadata in datasets}
    data_service = MagicMock(datasets=datasets)
    data_service.get_uploaded_dataset_ids.return_value = list(by_name)
    data_service.get_dataset_metadata.side_effect = by_name.get
    standards_context = MagicMock()
    standards_context.within_rule_scope.side_effect = lambda rule, metadata: (
        (True, "") if scope[metadata.name] else (False, "out of scope")
    )
    return SQLRulesEngine(data_service=data_service, standards_context=standards_context)


def test_engine_reports_in_scope_datasets_as_not_applicable_without_running_rule():
    engine = _engine({"AE": True, "DM": False})
    engine.validate_single_dataset = MagicMock()
    results = engine.sql_validate_single_rule(
        _rule(), not_applicable_reason="Not applicable: requires a define.xml (-dxp)"
    )
    engine.validate_single_dataset.assert_not_called()
    assert [(r["executionStatus"], r["message"]) for r in results["AE"]] == [
        (ExecutionStatus.SKIPPED.value, "Not applicable: requires a define.xml (-dxp)")
    ]
    assert [(r["executionStatus"], r["message"]) for r in results["DM"]] == [
        (ExecutionStatus.SKIPPED.value, "out of scope")
    ]


def test_engine_runs_rule_when_no_input_is_missing():
    engine = _engine({"AE": True})
    engine.validate_single_dataset = MagicMock(return_value=[{"executionStatus": "success", "errors": []}])
    results = engine.sql_validate_single_rule(_rule())
    engine.validate_single_dataset.assert_called_once()
    assert results["AE"] == [{"executionStatus": "success", "errors": []}]


def test_cli_rule_with_missing_input_is_skipped_with_reason():
    engine = _engine({"AE": True})
    engine.validate_single_dataset = MagicMock()
    args = MagicMock(progress="disabled")
    rule = _rule(rule_type="Variable Metadata Check against Define XML")
    rule.update(standards=[], actions=[{"name": "generate_dataset_error_objects", "params": {"message": "msg"}}])
    result = rsv.sql_validate_single_rule(engine, args, rule, [DEFINE_XML])
    engine.validate_single_dataset.assert_not_called()
    assert result.execution_status == ExecutionStatus.SKIPPED.value
    assert [r["message"] for r in result.results] == ["Not applicable: requires a define.xml (-dxp)"]


def test_is_not_applicable():
    assert is_not_applicable({"executionStatus": "skipped", "message": get_not_applicable_reason([DEFINE_XML])})
    assert not is_not_applicable({"executionStatus": "skipped", "message": "Rule skipped - doesn't apply to domain"})
    assert not is_not_applicable({"executionStatus": "success", "message": None})
    assert not is_not_applicable("error string")
