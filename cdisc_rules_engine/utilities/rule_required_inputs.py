"""
Works out which optional inputs (define.xml, STF file, external dictionaries) a rule needs,
so a rule can be reported as not applicable instead of failing when the input wasn't supplied.

Only rule type, operator/operation names, external_dictionary_type and define_* column names are
inspected, never literal values: some rules mention e.g. "UNII" or "SNOMED" as values and don't need
those dictionaries.
"""

from typing import Iterable, List, Set

from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes

DEFINE_XML = "define_xml"
STF_FILE = "stf_file"

REQUIRED_INPUT_DESCRIPTIONS = {
    DEFINE_XML: "a define.xml (-dxp)",
    STF_FILE: "a study tagging file (-stf)",
    DictionaryTypes.MEDDRA.value: "the MedDRA dictionary (--meddra)",
    DictionaryTypes.WHODRUG.value: "the WHODrug dictionary (--whodrug)",
    DictionaryTypes.LOINC.value: "the LOINC dictionary (--loinc)",
    DictionaryTypes.MEDRT.value: "the MED-RT dictionary (--medrt)",
    DictionaryTypes.UNII.value: "the UNII dictionary (--unii)",
    DictionaryTypes.SNOMED.value: "the SNOMED dictionary (--snomed)",
}

NOT_APPLICABLE_PREFIX = "Not applicable: requires"

# These take an external_dictionary_type but don't read the loaded dictionary: one reads the define.xml,
# the others compare against the engine's built-in table of dictionary release dates
DICTIONARY_FREE_OPERATORS = {
    "get_define_external_dictionary_version",
    "is_latest_available_external_dictionary_version",
    "is_not_latest_available_external_dictionary_version",
}


def _collect_names(obj, operators: Set[str], columns: Set[str], dictionary_types: Set[str], operator: str = None):
    # operator is the nearest enclosing one: a parsed condition keeps its parameters in a nested "value" dict
    if isinstance(obj, dict):
        if isinstance(obj.get("operator"), str):
            operator = obj["operator"].lower()
            operators.add(operator)
        for key in ("name", "target"):
            if isinstance(obj.get(key), str):
                columns.add(obj[key].lower())
        if isinstance(obj.get("external_dictionary_type"), str) and operator not in DICTIONARY_FREE_OPERATORS:
            dictionary_types.add(obj["external_dictionary_type"].lower())
        for value in obj.values():
            _collect_names(value, operators, columns, dictionary_types, operator)
    elif isinstance(obj, list):
        for value in obj:
            _collect_names(value, operators, columns, dictionary_types, operator)


def get_required_inputs(rule: dict) -> Set[str]:
    """Returns the optional inputs the rule needs, as keys of REQUIRED_INPUT_DESCRIPTIONS."""
    operators, columns, dictionary_types = set(), set(), set()
    _collect_names([rule.get("conditions"), rule.get("operations")], operators, columns, dictionary_types)
    rule_type = (rule.get("rule_type") or "").lower()
    required = set()
    if (
        "define" in rule_type
        or any("define" in operator for operator in operators)
        or any(column.startswith("define_") for column in columns)
    ):
        required.add(DEFINE_XML)
    for dictionary_type in DictionaryTypes.values():
        if dictionary_type in dictionary_types or any(dictionary_type in operator for operator in operators):
            required.add(dictionary_type)
    if "stf" in rule_type or any("stf" in operator for operator in operators):
        required.add(STF_FILE)
    return required


def get_supplied_inputs(define_xml_path: str, stf_file_path: str, dictionary_types: Iterable[str]) -> Set[str]:
    supplied = set(dictionary_types or [])
    if define_xml_path:
        supplied.add(DEFINE_XML)
    if stf_file_path:
        supplied.add(STF_FILE)
    return supplied


def get_missing_inputs(rule: dict, supplied_inputs: Set[str]) -> List[str]:
    """Returns the rule's required inputs that weren't supplied, in a stable order."""
    required = get_required_inputs(rule)
    return [key for key in REQUIRED_INPUT_DESCRIPTIONS if key in required and key not in supplied_inputs]


def get_not_applicable_reason(missing_inputs: List[str]) -> str:
    descriptions = [REQUIRED_INPUT_DESCRIPTIONS[key] for key in missing_inputs]
    return f"{NOT_APPLICABLE_PREFIX} {' and '.join(descriptions)}"


def _rules(count: int) -> str:
    return f"{count} rule{'s' if count != 1 else ''}"


def is_not_applicable(result_entry) -> bool:
    """True for a dataset result that was skipped because the rule needs an input that wasn't supplied."""
    return isinstance(result_entry, dict) and str(result_entry.get("message") or "").startswith(NOT_APPLICABLE_PREFIX)


def get_missing_inputs_summary(missing_inputs_by_rule: dict) -> str:
    """
    One console line, e.g. "35 rules were not applicable because an input was not supplied:
    33 rules require a define.xml (-dxp), 2 rules require the MedDRA dictionary (--meddra)".
    missing_inputs_by_rule maps core id -> list of missing input keys. Returns "" if nothing is missing.
    """
    rules_needing = {
        key: sum(key in missing for missing in missing_inputs_by_rule.values()) for key in REQUIRED_INPUT_DESCRIPTIONS
    }
    details = ", ".join(
        f"{_rules(count)} require{'s' if count == 1 else ''} {REQUIRED_INPUT_DESCRIPTIONS[key]}"
        for key, count in rules_needing.items()
        if count
    )
    total = sum(bool(missing) for missing in missing_inputs_by_rule.values())
    if not total:
        return ""
    verb = "was" if total == 1 else "were"
    return f"{_rules(total)} {verb} not applicable because an input was not supplied: {details}"
