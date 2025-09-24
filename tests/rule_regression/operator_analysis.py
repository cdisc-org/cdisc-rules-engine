from typing import Set, Dict, List
from pathlib import Path
import json
import inspect
from cdisc_rules_engine.check_operators.sql.postgresql_operators import PostgresQLOperators


def find_rules_without_operators() -> List[Dict]:
    """
    Find rules that don't have any check operators.
    Args:
        rules_dir: Directory containing the rules JSON files
    Returns:
        List of rules without check operators, each with core_id and rule_id
    """
    rules_path = Path("tests/resources/rules") / "rules.json"
    if not rules_path.exists():
        return []

    with open(rules_path, "r") as f:
        all_rules = json.load(f)

    rules_without_operators = []
    for rule in all_rules:
        if not rule.get("check_operators"):
            rules_without_operators.append(
                {
                    "core_id": rule.get("core-id", "N/A"),
                    "cdisc_rule_id": rule.get("cdisc_rule_id", "N/A"),
                    "standard": rule.get("standard", "N/A"),
                    "status": rule.get("status", "N/A"),
                    "in_cache": rule.get("in_cache", False),
                }
            )

    return rules_without_operators


def check_operator_implementation_status() -> Dict[str, bool]:
    """
    Check which operators are actually implemented vs just raising NotImplementedError.
    This version uses a mock data service to avoid instantiation issues.
    Returns:
        Dict mapping operator names to their implementation status (True = implemented, False = not implemented)
    """
    implementation_status = {}

    from unittest.mock import MagicMock

    for operator_name, operator_factory in PostgresQLOperators._operator_map.items():
        try:
            mock_data_service = MagicMock()
            mock_data_service.get_dataset_metadata.return_value = {}

            dummy_data = {
                "df": None,
                "dataset_id": "test",
                "data_service": mock_data_service,
                "column_prefix_map": {},
                "value_level_metadata": [],
                "column_codelist_map": {},
                "codelist_term_maps": [],
                "operation_variables": {},
            }

            operator_instance = operator_factory(dummy_data)
            if hasattr(operator_instance, "execute_operator"):
                method = operator_instance.execute_operator

                try:
                    source = inspect.getsource(method)
                    is_implemented = "NotImplementedError" not in source
                except (OSError, TypeError):
                    # If we can't get source, assume it's implemented
                    is_implemented = True
            else:
                # If no execute_operator method, consider it not implemented
                is_implemented = False

            implementation_status[operator_name] = is_implemented

        except Exception:
            # If we can't instantiate or check the operator, assume it's not implemented
            implementation_status[operator_name] = False

    return implementation_status


def generate_operators_analysis_report(operators_in_rules: Set[str]) -> None:
    """
    Generate a markdown report analyzing operator implementation status.
    Args:
        operators_in_rules: Set of operator names found in rules
    """
    all_operators_list = sorted(list(operators_in_rules))

    # Check which operators are actually implemented (not raising NotImplementedError)
    implementation_status = check_operator_implementation_status()
    implemented_operators = {op for op, is_impl in implementation_status.items() if is_impl}
    print(f"Implemented operators: {implemented_operators}")

    missing_operators = sorted(list(set(all_operators_list) - implemented_operators))
    extra_operators = sorted(list(implemented_operators - set(all_operators_list)))

    rules_without_operators = find_rules_without_operators()

    analysis_report = []
    analysis_report.append("# 🔍 Check Operators Analysis Report")
    analysis_report.append("")

    analysis_report.append("## 📊 Summary")
    analysis_report.append("")
    analysis_report.append(f"- Total rules without check operators: {len(rules_without_operators)}")
    analysis_report.append(f"- Total unique operators found in rules: {len(all_operators_list)}")
    analysis_report.append(f"- Total implemented PostgreSQL operators: {len(implemented_operators)}")
    analysis_report.append(f"- Missing operators (in rules but not implemented): {len(missing_operators)}")
    analysis_report.append(f"- Extra operators (implemented but not used in rules): {len(extra_operators)}")
    analysis_report.append("")

    analysis_report.append("## ✅ Implementation Status")
    analysis_report.append("")
    if missing_operators:
        analysis_report.append("### ❌ Missing Operators (Need Implementation)")
        analysis_report.append("")
        analysis_report.append("| Operator | Status |")
        analysis_report.append("|----------|--------|")
        for op in missing_operators:
            analysis_report.append(f"| `{op}` | ❌ Not Implemented |")
        analysis_report.append("")
    else:
        analysis_report.append("### 🎉 All Rule Operators Implemented")
        analysis_report.append("")
        analysis_report.append("Great news! All operators used in rules have been implemented in PostgreSQL.")
        analysis_report.append("")

    if extra_operators:
        analysis_report.append("## 🔍 Unused Implemented Operators")
        analysis_report.append("")
        analysis_report.append("The following operators are implemented but not currently used in any rules:")
        analysis_report.append("")
        analysis_report.append("```")
        for op in extra_operators:
            analysis_report.append(f"- {op}")
        analysis_report.append("```")
        analysis_report.append("")

    if rules_without_operators:
        analysis_report.append("## 📋 Rules Without Check Operators")
        analysis_report.append("")
        analysis_report.append("| Core ID | CDISC Rule ID | Standard | Status | In Cache |")
        analysis_report.append("|---------|---------------|----------|--------|----------|")
        for rule in rules_without_operators:
            cache_status = "✅" if rule["in_cache"] else "❌"
            row = (
                f"| {rule['core_id']} | {rule['cdisc_rule_id']} | "
                f"{rule['standard']} | {rule['status']} | {cache_status} |"
            )
            analysis_report.append(row)
        analysis_report.append("")

    output_path = Path("tests/resources/rules") / "operators_analysis_report.md"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(analysis_report))

    # Format the report with Prettier
    import subprocess

    try:
        subprocess.run(["npx", "prettier", "--write", str(output_path)], check=True)
    except subprocess.CalledProcessError:
        pass
