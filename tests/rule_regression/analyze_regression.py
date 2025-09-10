#!/usr/bin/env python3
"""
Regression Analysis Script for CDISC Rules Engine
Analyzes tests/resources/rules/rules.json to identify missing operators and error patterns
"""

import json
import re
from collections import defaultdict, Counter
from typing import Dict, List


def _extract_errors_from_results(results_sql: List[Dict], pattern: re.Pattern) -> List[str]:
    """Helper: Extract matching error messages from SQL results"""
    matches = []
    for result in results_sql:
        if result.get("execution_status") == "execution_error":
            errors = result.get("errors", [])
            for error in errors:
                message = error.get("message", "")
                match = pattern.search(message)
                if match:
                    matches.append(match.group(1))
    return matches


def _process_test_cases(rule: Dict, test_type: str, pattern: re.Pattern) -> List[str]:
    """Helper: Process test cases and extract pattern matches"""
    matches = []
    for test_case in rule.get(test_type, []):
        for test_case_key, test_case_data in test_case.items():
            if not isinstance(test_case_data, dict):
                continue
            engine_regression = test_case_data.get("engine_regression")
            if not engine_regression:
                continue

            results_sql = engine_regression.get("results_sql", [])
            matches.extend(_extract_errors_from_results(results_sql, pattern))
    return matches


def extract_operator_failures(rules_data: List[Dict]) -> tuple[Dict[str, int], Dict[str, int]]:
    """Extract missing operators and count their failures"""
    operator_failures = defaultdict(int)
    operator_rules = defaultdict(set)
    operator_pattern = re.compile(r"(\w+) check_operator not implemented")

    for rule in rules_data:
        core_id = rule.get("core-id", "unknown")
        for test_type in ["negative_regressions", "positive_regressions"]:
            matches = _process_test_cases(rule, test_type, operator_pattern)
            for operator_name in matches:
                operator_failures[operator_name] += 1
                operator_rules[operator_name].add(core_id)

    operator_rule_counts = {op: len(rules) for op, rules in operator_rules.items()}

    return dict(operator_failures), operator_rule_counts


def categorize_skip_types(skip_messages: List[str]) -> Dict[str, int]:
    """Categorize different types of skip messages"""
    skip_categories = defaultdict(int)

    for msg in skip_messages:
        if not msg:
            continue
        msg_lower = msg.lower()
        if "doesn't apply to class" in msg_lower:
            skip_categories["class_not_applicable"] += 1
        elif "doesn't apply to domain" in msg_lower:
            skip_categories["domain_not_applicable"] += 1
        elif "doesn't apply to" in msg_lower:
            skip_categories["other_not_applicable"] += 1
        elif "rule skipped" in msg_lower:
            skip_categories["other_skip_reason"] += 1
        else:
            skip_categories["unknown_skip"] += 1

    return dict(skip_categories)


def _extract_first_error_message(results: List[Dict]) -> str:
    """Helper: Extract first error message from results"""
    for result in results:
        if result.get("execution_status") == "execution_error":
            errors = result.get("errors", [])
            if errors:
                return errors[0].get("message", "Unknown error")
    return None


def _extract_first_skip_message(results: List[Dict]) -> str:
    """Helper: Extract first skip message from results"""
    for result in results:
        if result.get("execution_status") == "skipped":
            return result.get("execution_message", "Unknown skip reason")
    return None


def _classify_discrepancy(sql_statuses: set, old_statuses: set) -> str:
    """Helper: Classify type of discrepancy"""
    if "execution_error" in sql_statuses and "skipped" in old_statuses:
        return "sql_error_old_skipped"
    elif "success" in sql_statuses and "skipped" in old_statuses:
        return "sql_success_old_skipped"
    elif "execution_error" in sql_statuses and "success" in old_statuses:
        return "sql_error_old_success"
    return None


def _process_discrepancy_case(core_id: str, engine_regression: Dict) -> Dict:
    """Helper: Process a single discrepancy case"""
    old_vs_sql = engine_regression.get("old_vs_sql", {})
    if old_vs_sql.get("execution_status_match", True):
        return None

    results_sql = engine_regression.get("results_sql", [])
    results_old = engine_regression.get("results_old", [])

    sql_statuses = set(r.get("execution_status") for r in results_sql)
    old_statuses = set(r.get("execution_status") for r in results_old)

    discrepancy_type = _classify_discrepancy(sql_statuses, old_statuses)
    if not discrepancy_type:
        return None

    return {
        "type": discrepancy_type,
        "info": {
            "core_id": core_id,
            "sql_error_msg": _extract_first_error_message(results_sql),
            "old_skip_msg": _extract_first_skip_message(results_old),
        },
    }


def analyze_meaningful_discrepancies(rules_data: List[Dict]) -> Dict[str, List[Dict]]:
    """Analyze SQL vs Old discrepancies that reveal actual problems"""
    discrepancy_patterns = {"sql_error_old_skipped": [], "sql_success_old_skipped": [], "sql_error_old_success": []}

    for rule in rules_data:
        core_id = rule.get("core-id", "unknown")

        for test_type in ["negative_regressions", "positive_regressions"]:
            for test_case in rule.get(test_type, []):
                for test_case_key, test_case_data in test_case.items():
                    if not isinstance(test_case_data, dict):
                        continue
                    engine_regression = test_case_data.get("engine_regression")
                    if not engine_regression:
                        continue

                    discrepancy = _process_discrepancy_case(core_id, engine_regression)
                    if discrepancy:
                        discrepancy_patterns[discrepancy["type"]].append(discrepancy["info"])

    return discrepancy_patterns


def extract_missing_operations(rules_data: List[Dict]) -> tuple[Dict[str, int], Dict[str, int]]:
    """Extract missing operations and count their failures"""
    operation_failures = defaultdict(int)
    operation_rules = defaultdict(set)
    operation_pattern = re.compile(r"Operation (\w+) is not implemented")

    for rule in rules_data:
        core_id = rule.get("core-id", "unknown")
        for test_type in ["negative_regressions", "positive_regressions"]:
            matches = _process_test_cases(rule, test_type, operation_pattern)
            for operation_name in matches:
                operation_failures[operation_name] += 1
                operation_rules[operation_name].add(core_id)

    operation_rule_counts = {op: len(rules) for op, rules in operation_rules.items()}

    return dict(operation_failures), operation_rule_counts


def _check_error_message_type(
    message: str, operator_pattern: re.Pattern, operation_pattern: re.Pattern
) -> tuple[bool, bool, bool]:
    """Helper: Classify a single error message"""
    has_operator_error = bool(operator_pattern.search(message))
    has_operation_error = bool(operation_pattern.search(message))
    has_other_error = _is_other_error(message) if not (has_operator_error or has_operation_error) else False
    return has_operator_error, has_operation_error, has_other_error


def _process_test_case_errors(
    test_case_data: Dict, operator_pattern: re.Pattern, operation_pattern: re.Pattern
) -> tuple[bool, bool, bool]:
    """Helper: Process errors in a single test case"""
    has_operator_error = False
    has_operation_error = False
    has_other_error = False

    engine_regression = test_case_data.get("engine_regression")
    if not engine_regression:
        return has_operator_error, has_operation_error, has_other_error

    results_sql = engine_regression.get("results_sql", [])
    for result in results_sql:
        if result.get("execution_status") == "execution_error":
            errors = result.get("errors", [])
            for error in errors:
                message = error.get("message", "")
                op_err, operation_err, other_err = _check_error_message_type(
                    message, operator_pattern, operation_pattern
                )
                has_operator_error = has_operator_error or op_err
                has_operation_error = has_operation_error or operation_err
                has_other_error = has_other_error or other_err

    return has_operator_error, has_operation_error, has_other_error


def _check_rule_error_types(
    rule: Dict, operator_pattern: re.Pattern, operation_pattern: re.Pattern
) -> tuple[bool, bool, bool]:
    """Helper: Check what error types a single rule has"""
    has_operator_error = False
    has_operation_error = False
    has_other_error = False

    for test_type in ["negative_regressions", "positive_regressions"]:
        for test_case in rule.get(test_type, []):
            for test_case_key, test_case_data in test_case.items():
                if not isinstance(test_case_data, dict):
                    continue

                op_err, operation_err, other_err = _process_test_case_errors(
                    test_case_data, operator_pattern, operation_pattern
                )
                has_operator_error = has_operator_error or op_err
                has_operation_error = has_operation_error or operation_err
                has_other_error = has_other_error or other_err

    return has_operator_error, has_operation_error, has_other_error


def analyze_rules_by_error_type(rules_data: List[Dict]) -> Dict[str, set]:
    """Categorize rules by the types of errors they have"""
    rules_with_operator_errors = set()
    rules_with_operation_errors = set()
    rules_with_other_errors = set()

    operator_pattern = re.compile(r"(\w+) check_operator not implemented")
    operation_pattern = re.compile(r"Operation (\w+) is not implemented")

    for rule in rules_data:
        core_id = rule.get("core-id", "unknown")
        has_operator_error, has_operation_error, has_other_error = _check_rule_error_types(
            rule, operator_pattern, operation_pattern
        )

        if has_operator_error:
            rules_with_operator_errors.add(core_id)
        if has_operation_error:
            rules_with_operation_errors.add(core_id)
        if has_other_error:
            rules_with_other_errors.add(core_id)

    return {
        "operator_errors": rules_with_operator_errors,
        "operation_errors": rules_with_operation_errors,
        "other_errors": rules_with_other_errors,
    }


def _is_other_error(message: str) -> bool:
    """Helper: Check if message is an 'other' error (not operator/operation)"""
    return "check_operator not implemented" not in message and not (
        message.startswith("Operation") and "is not implemented" in message
    )


def _extract_error_types_from_results(results_sql: List[Dict]) -> List[str]:
    """Helper: Extract error types from SQL results using the 'error' field"""
    error_types = []
    for result in results_sql:
        if result.get("execution_status") == "execution_error":
            errors = result.get("errors", [])
            for error in errors:
                error_type = error.get("error", "Unknown error type")
                error_types.append(error_type)
    return error_types


def _extract_other_errors_from_results(results_sql: List[Dict]) -> List[str]:
    """Helper: Extract other error messages from SQL results"""
    messages = []
    for result in results_sql:
        if result.get("execution_status") == "execution_error":
            errors = result.get("errors", [])
            for error in errors:
                message = error.get("message", "")
                if _is_other_error(message):
                    messages.append(message)
    return messages


def categorize_execution_errors(rules_data: List[Dict]) -> tuple[Counter, Dict[str, int]]:
    """Categorize execution errors by error type (using our improved error field)"""
    error_types = []
    error_rules = defaultdict(set)

    for rule in rules_data:
        core_id = rule.get("core-id", "unknown")
        for test_type in ["negative_regressions", "positive_regressions"]:
            for test_case in rule.get(test_type, []):
                for test_case_key, test_case_data in test_case.items():
                    if not isinstance(test_case_data, dict):
                        continue
                    engine_regression = test_case_data.get("engine_regression")
                    if not engine_regression:
                        continue

                    results_sql = engine_regression.get("results_sql", [])
                    error_type_list = _extract_error_types_from_results(results_sql)
                    error_types.extend(error_type_list)
                    for error_type in error_type_list:
                        error_rules[error_type].add(core_id)

    # Convert sets to counts
    error_rule_counts = {error_type: len(rules) for error_type, rules in error_rules.items()}

    return Counter(error_types), error_rule_counts


def _generate_operators_section(operator_failures: Dict[str, int], operator_rule_counts: Dict[str, int]) -> List[str]:
    """Helper: Generate missing operators section"""
    section = []
    if operator_failures:
        total_failures = sum(operator_failures.values())
        total_rules_affected = sum(operator_rule_counts.values())
        section.append(
            f"## Missing Operators ({len(operator_failures)} operators, {total_failures} total failures "
            f"across {total_rules_affected} rule occurrences)"
        )
        section.append("")

        # Sort by rule count first, then by failure count
        sorted_operators = sorted(
            operator_failures.items(), key=lambda x: (operator_rule_counts.get(x[0], 0), x[1]), reverse=True
        )
        for i, (operator, count) in enumerate(sorted_operators, 1):
            rule_count = operator_rule_counts.get(operator, 0)
            section.append(f"{i:2d}. **{operator}**: {count} failures across {rule_count} rules")
    else:
        section.append("## Missing Operators")
        section.append("No missing operator errors found!")
    return section


def _generate_operations_section(
    operation_failures: Dict[str, int], operation_rule_counts: Dict[str, int]
) -> List[str]:
    """Helper: Generate missing operations section"""
    section = []
    if operation_failures:
        total_op_failures = sum(operation_failures.values())
        total_rules_affected = sum(operation_rule_counts.values())
        section.append(
            f"\n## Missing Operations ({len(operation_failures)} operations, {total_op_failures} total failures "
            f"across {total_rules_affected} rule occurrences)"
        )
        section.append("")

        # Sort by rule count first, then by failure count
        sorted_operations = sorted(
            operation_failures.items(), key=lambda x: (operation_rule_counts.get(x[0], 0), x[1]), reverse=True
        )
        for i, (operation, count) in enumerate(sorted_operations, 1):
            rule_count = operation_rule_counts.get(operation, 0)
            section.append(f"{i:2d}. **{operation}**: {count} failures across {rule_count} rules")
    else:
        section.append("\n## Missing Operations")
        section.append("No missing operation errors found!")
    return section


def _generate_discrepancies_section(discrepancies: Dict[str, List[Dict]]) -> List[str]:
    """Helper: Generate discrepancies section"""
    section = []
    section.append("\n## SQL vs Old Engine Discrepancies")
    section.append("")

    # SQL Error vs Old Skipped
    sql_error_old_skipped = discrepancies["sql_error_old_skipped"]
    if sql_error_old_skipped:
        section.append(f"### SQL Errors where Old Engine Skipped ({len(sql_error_old_skipped)} cases)")
        section.append("*Indicates SQL engine running rules it shouldn't*")

        error_patterns = Counter(d["sql_error_msg"] for d in sql_error_old_skipped if d["sql_error_msg"])
        for error_msg, count in error_patterns.most_common(10):
            short_msg = error_msg[:60] + "..." if len(error_msg) > 60 else error_msg
            section.append(f"- [{count}] {short_msg}")
        section.append("")

    # SQL Success vs Old Skipped
    sql_success_old_skipped = discrepancies["sql_success_old_skipped"]
    if sql_success_old_skipped:
        section.append(f"### SQL Success where Old Engine Skipped ({len(sql_success_old_skipped)} cases)")
        section.append("*Indicates SQL engine not respecting rule applicability*")

        skip_messages = [d["old_skip_msg"] for d in sql_success_old_skipped if d["old_skip_msg"]]
        skip_categories = categorize_skip_types(skip_messages)

        section.append("**Skip Types:**")
        for skip_type, count in sorted(skip_categories.items(), key=lambda x: x[1], reverse=True):
            skip_type_readable = skip_type.replace("_", " ").title()
            section.append(f"- {skip_type_readable}: {count}")

        skip_patterns = Counter(d["old_skip_msg"] for d in sql_success_old_skipped if d["old_skip_msg"])
        section.append("\n**Examples:**")
        for skip_msg, count in skip_patterns.most_common(5):
            short_msg = skip_msg[:60] + "..." if len(skip_msg) > 60 else skip_msg
            section.append(f"- [{count}] {short_msg}")
        section.append("")

    # SQL Error vs Old Success
    sql_error_old_success = discrepancies["sql_error_old_success"]
    if sql_error_old_success:
        section.append(f"### SQL Errors where Old Engine Succeeded ({len(sql_error_old_success)} cases)")
        section.append("*Indicates actual regressions in SQL implementation*")

        error_patterns = Counter(d["sql_error_msg"] for d in sql_error_old_success if d["sql_error_msg"])
        for error_msg, count in error_patterns.most_common(10):
            short_msg = error_msg[:60] + "..." if len(error_msg) > 60 else error_msg
            section.append(f"- [{count}] {short_msg}")
        section.append("")

    return section


def _generate_execution_errors_section(
    execution_errors: Counter, execution_error_rule_counts: Dict[str, int]
) -> List[str]:
    """Helper: Generate execution errors section by error type"""
    section = []
    if execution_errors:
        total_failures = sum(execution_errors.values())
        total_rules_affected = sum(execution_error_rule_counts.values())
        section.append(
            f"## Execution Errors by Type ({len(execution_errors)} unique error types, {total_failures} total failures "
            f"across {total_rules_affected} rule occurrences)"
        )
        section.append("")

        # Sort by rule count first, then by failure count
        sorted_errors = sorted(
            execution_errors.items(), key=lambda x: (execution_error_rule_counts.get(x[0], 0), x[1]), reverse=True
        )
        for i, (error_type, count) in enumerate(sorted_errors[:15], 1):
            rule_count = execution_error_rule_counts.get(error_type, 0)
            section.append(f"{i:2d}. **{error_type}**: {count} failures across {rule_count} rules")
        section.append("")
    return section


def _generate_rule_summary_section(rules_by_error_type: Dict[str, set], total_rules: int) -> List[str]:
    """Helper: Generate rule summary section showing unique rules with errors"""
    section = []
    section.append(f"## Rule Error Summary (out of {total_rules} total rules)")
    section.append("")

    operator_rules = rules_by_error_type["operator_errors"]
    operation_rules = rules_by_error_type["operation_errors"]
    other_rules = rules_by_error_type["other_errors"]

    # Calculate total unique rules with any error
    all_error_rules = operator_rules | operation_rules | other_rules
    clean_rules = total_rules - len(all_error_rules)

    section.append(
        f"- **Rules with any errors**: {len(all_error_rules)} ({len(all_error_rules) / total_rules * 100:.1f}%)"
    )
    section.append(f"- **Clean rules**: {clean_rules} ({clean_rules / total_rules * 100:.1f}%)")
    section.append("")

    section.append("**Error Breakdown by Category:**")
    section.append(f"- Rules with **operator errors**: {len(operator_rules)}")
    section.append(f"- Rules with **operation errors**: {len(operation_rules)}")
    section.append(f"- Rules with **other errors**: {len(other_rules)}")

    return section


def generate_report(rules_data: List[Dict]) -> str:
    """Generate focused analysis report"""
    report = []
    report.append("# CDISC Rules Engine Regression Analysis")
    report.append("=" * 45)
    report.append("")

    # Generate all sections
    operator_failures, operator_rule_counts = extract_operator_failures(rules_data)
    operation_failures, operation_rule_counts = extract_missing_operations(rules_data)
    rules_by_error_type = analyze_rules_by_error_type(rules_data)
    discrepancies = analyze_meaningful_discrepancies(rules_data)
    execution_errors, execution_error_rule_counts = categorize_execution_errors(rules_data)

    # Add rule summary at the top
    report.extend(_generate_rule_summary_section(rules_by_error_type, len(rules_data)))
    report.append("")

    report.extend(_generate_operators_section(operator_failures, operator_rule_counts))
    report.extend(_generate_operations_section(operation_failures, operation_rule_counts))
    report.extend(_generate_execution_errors_section(execution_errors, execution_error_rule_counts))
    report.extend(_generate_discrepancies_section(discrepancies))

    return "\n".join(report)


def main():
    """Main analysis function"""
    import os

    # Use os.path.join for cross-platform compatibility
    script_dir = os.path.dirname(os.path.abspath(__file__))
    rules_file = os.path.join(script_dir, "..", "resources", "rules", "rules.json")

    print("Loading rules data...")
    try:
        with open(rules_file, "r", encoding="utf-8") as f:
            rules_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Could not find {rules_file}")
        print("Make sure you're running this script from the tests/rule_regression directory.")
        return

    print(f"Analyzing {len(rules_data)} rules...")
    report = generate_report(rules_data)

    # Save report to file
    output_file = os.path.join(script_dir, "..", "resources", "rules", "regression_analysis_report.md")
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"\nAnalysis complete! Report saved to: {output_file}")
    print("\n" + "=" * 50)
    print(report)


if __name__ == "__main__":
    main()
