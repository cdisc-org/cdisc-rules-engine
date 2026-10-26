"""Which columns an error reports for a rule, otherwise known as "targets"."""

import re
from typing import Dict, List, Optional

from cdisc_rules_engine.interfaces.condition_interface import ConditionInterface
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema

# Operators whose target is really a family of columns: TSVAL -> TSVAL1, TSVAL2, ...
ENUMERATED_TARGET_OPERATORS = {"additional_columns_empty", "additional_columns_not_empty"}


def get_target_columns(rule: dict, metadata, schema: SqlTableSchema) -> Dict[str, bool]:
    """
    Target name -> whether the rule's source actually has it.
    $operation outputs always count as present.
    """
    return {
        column: True if column.startswith("$") else schema.has_column(column)
        for column in extract_target_names(rule, metadata, schema)
    }


def extract_target_names(rule: dict, metadata, schema: SqlTableSchema) -> List[str]:
    """
    output_variables if the rule declares them.
    Otherwise the target of each check.
    """
    domain = metadata.domain or ""
    output_variables: List[str] = rule.get("output_variables", [])
    if output_variables:
        # only the first "--" - output variables are a single prefixed name
        return [variable.replace("--", domain, 1) for variable in output_variables]

    target_names: List[str] = []
    conditions: ConditionInterface = rule["conditions"]
    for condition in conditions.values():
        if condition.get("operator") == "not_exists":
            continue
        target: str = condition["value"].get("target")
        if target is None:
            continue
        target = target.replace("--", domain)
        pattern = operator_related_pattern(condition.get("operator"), target)
        if pattern is None:
            target_names.append(target)
            continue
        columns = [column for column, _ in schema.get_columns()]
        target_names.extend(name for name in columns if re.match(pattern, name))
    return target_names


def operator_related_pattern(operator: str, target: str) -> Optional[str]:
    if operator in ENUMERATED_TARGET_OPERATORS:
        return rf"^{target}\d+$"
    return None
