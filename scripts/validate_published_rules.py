#!/usr/bin/env python3
"""
validate_published_rules.py

Validates every Published rule from cdisc-open-rules against cdisc-rules-engine.
Intended to be called from the CI workflow (validate-published-rules.yml) instead
of the large inline bash block.

Outputs:
    <output-dir>/summary_table.md   — per-test-case results table
    <output-dir>/detail_report.md   — failure details (stderr / diffs)

Exit code:
    0  — all rules passed
    1  — one or more rules failed
"""

import argparse
import json
import os
import subprocess
import sys

SUMMARY_HEADERS = ["Rule", "Type", "Number", "Execution", "Expected", "Actual", "Match"]
CHECKMARK = "\u2705"
CROSS = "\u274c"


# ---------------------------------------------------------------------------
# Markdown helpers
# ---------------------------------------------------------------------------


def create_md_table(table_name, headers, records, property_getter=None):
    """
    Create a Markdown table with the given headers and records.

    Args:
        table_name: The title of the table
        headers: List of column headers
        records: List of records to include in the table
        property_getter: Optional function to extract properties from records.
                         If None, assumes records are dictionaries.
    Returns:
        String containing the formatted Markdown table
    """
    title = f"### {table_name}"
    header = "| " + " | ".join(headers) + " |"
    underline = "| " + " | ".join(["---" for _ in headers]) + " |"

    if property_getter is None:

        def property_getter(record, prop):
            return str(record.get(prop, ""))

    values = "\n".join(
        "| " + " | ".join([property_getter(record, prop) for prop in headers]) + " |"
        for record in records
    )

    return f"{title}\n\n{header}\n{underline}\n{values}"


def _parse_case_result(line: str) -> dict:
    """
    Parse one JSON line from case_results.jsonl produced by run_validation.sh.

    Returns a flat dict with both display-ready values (keyed by SUMMARY_HEADERS)
    and private fields (prefixed with '_') needed to build failure detail sections.
    """
    d = json.loads(line)
    exec_ok = bool(d["exec"])
    match_ok = bool(d.get("match", False))
    return {
        # Display fields — keys match SUMMARY_HEADERS exactly
        "Rule": d["rule"],
        "Type": d["type"],
        "Number": str(d["num"]),
        "Execution": CHECKMARK if exec_ok else CROSS,
        "Expected": str(d.get("expected", "")),
        "Actual": str(d.get("got", "")),
        "Match": CHECKMARK if match_ok else CROSS,
        # Private fields used when generating failure detail
        "_exec_ok": exec_ok,
        "_match_ok": match_ok,
        "_diff_file": d.get("diff", "") or "",
        "_stderr_file": d.get("stderr", "") or "",
    }


def _build_failure_detail(record: dict) -> str:
    """Return a Markdown section describing one failing test case."""
    lines = [f"## {record['Rule']} \u2014 {record['Type']} / {record['Number']}\n"]
    if not record["_exec_ok"]:
        lines.append("**Execution failed.**\n")
        stderr_file = record["_stderr_file"]
        if stderr_file and os.path.isfile(stderr_file):
            lines.append("```")
            with open(stderr_file) as fh:
                lines.append(fh.read())
            lines.append("```")
    else:
        lines.append(
            f"**Expected:** {record['Expected']}  **Actual:** {record['Actual']}\n"
        )
        diff_file = record["_diff_file"]
        if diff_file and os.path.isfile(diff_file):
            lines.append("```diff")
            with open(diff_file) as fh:
                lines.append(fh.read())
            lines.append("```")
    lines.append("")
    return "\n".join(lines)


def _run_rule_validation(
    rule_id: str,
    scripts_dir: str,
    rules_root: str,
    engine_dir: str,
    python_cmd: str,
) -> int:
    """
    Invoke run_validation.sh for a single rule directory and return its exit code.
    Output is streamed directly to stdout/stderr so CI logs remain readable.
    """
    env = os.environ.copy()
    env["ENGINE_DIR_OVERRIDE"] = engine_dir

    result = subprocess.run(
        [
            "bash",
            os.path.join(scripts_dir, "run_validation.sh"),
            f"Published/{rule_id}",
            python_cmd,
            rules_root,
        ],
        env=env,
    )
    return result.returncode


def _process_case_results(case_results_path: str) -> tuple[list[dict], list[str], bool]:
    """
    Read and remove case_results.jsonl, returning:
      - summary rows (public fields only)
      - failure detail sections
      - whether any row failed
    """
    summary_rows: list[dict] = []
    details: list[str] = []
    any_failed = False

    with open(case_results_path) as fh:
        raw_lines = fh.readlines()
    os.remove(case_results_path)

    for raw_line in raw_lines:
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        record = _parse_case_result(raw_line)
        summary_rows.append({k: v for k, v in record.items() if not k.startswith("_")})
        if not record["_exec_ok"] or not record["_match_ok"]:
            any_failed = True
            details.append(_build_failure_detail(record))

    return summary_rows, details, any_failed


def _aggregate_row(
    rule_id: str, rule_exit: int, rules_root: str
) -> tuple[dict, str | None]:
    """
    Build a single-row summary entry and optional failure detail
    for a rule that produced no per-case JSONL output.
    """
    exec_ok = rule_exit == 0
    row = {
        "Rule": rule_id,
        "Type": "\u2014",
        "Number": "\u2014",
        "Execution": CHECKMARK if exec_ok else CROSS,
        "Expected": "\u2014",
        "Actual": "\u2014",
        "Match": "\u2014",
    }
    if exec_ok:
        return row, None

    detail = f"## {rule_id}\n\n"
    report_file = os.path.join(rules_root, "validation_report.md")
    if os.path.isfile(report_file):
        with open(report_file) as fh:
            detail += fh.read()
        detail += "\n"
    return row, detail


def _validate_one_rule(
    rule_id: str,
    scripts_dir: str,
    rules_root: str,
    engine_dir: str,
    python_cmd: str,
) -> tuple[list[dict], list[str], bool]:
    """
    Run validation for a single rule and return summary rows, failure details,
    and whether the rule passed.
    """
    print("=" * 40)
    print(f" Validating {rule_id}")
    print("=" * 40)

    rule_exit = _run_rule_validation(
        rule_id, scripts_dir, rules_root, engine_dir, python_cmd
    )

    case_results_path = os.path.join(rules_root, "case_results.jsonl")

    if os.path.isfile(case_results_path):
        summary_rows, details, row_failed = _process_case_results(case_results_path)
        passed = not row_failed and rule_exit == 0
    else:
        row, detail = _aggregate_row(rule_id, rule_exit, rules_root)
        summary_rows = [row]
        details = [detail] if detail is not None else []
        passed = rule_exit == 0

    # Clean up any leftover report file
    report_file = os.path.join(rules_root, "validation_report.md")
    if os.path.isfile(report_file):
        os.remove(report_file)

    return summary_rows, details, passed


def _write_reports(
    summary_records: list[dict],
    failure_details: list[str],
    rule_pass: int,
    rule_fail: int,
    output_dir: str,
) -> None:
    """Render and write summary_table.md and detail_report.md."""
    total = rule_pass + rule_fail
    totals_line = (
        f"**Total:** {total}  |  "
        f"{CHECKMARK} Passed: {rule_pass}  |  "
        f"{CROSS} Failed: {rule_fail}"
    )
    summary_md = (
        "# Published Rules Validation \u2014 Summary\n\n"
        f"{totals_line}\n\n"
        + create_md_table("Results", SUMMARY_HEADERS, summary_records)
    )
    detail_body = "\n".join(failure_details) if failure_details else "_No failures._\n"
    detail_md = f"# Published Rules Validation \u2014 Failure Details\n\n{detail_body}"

    os.makedirs(output_dir, exist_ok=True)
    summary_path = os.path.join(output_dir, "summary_table.md")
    detail_path = os.path.join(output_dir, "detail_report.md")

    with open(summary_path, "w", encoding="utf-8") as fh:
        fh.write(summary_md)
    with open(detail_path, "w", encoding="utf-8") as fh:
        fh.write(detail_md)

    print(f"\nSummary written to : {summary_path}")
    print(f"Details written to : {detail_path}")


def validate_all_rules(
    rules_root: str,
    engine_dir: str,
    python_cmd: str,
    output_dir: str,
) -> bool:
    """
    Iterate every directory under Published/, run the validation shell script,
    parse results, and write the two report files.

    Returns True if any rule failed, False if all passed.
    """
    published_dir = os.path.join(rules_root, "Published")
    scripts_dir = os.path.join(rules_root, ".github", "scripts")

    if not os.path.isdir(published_dir):
        print(f"ERROR: Published/ not found under {rules_root}", file=sys.stderr)
        return True

    rule_ids = sorted(
        entry
        for entry in os.listdir(published_dir)
        if os.path.isdir(os.path.join(published_dir, entry))
    )

    if not rule_ids:
        print("ERROR: No rule directories found under Published/", file=sys.stderr)
        return True

    print(f"Found {len(rule_ids)} rule(s) under Published/")

    summary_records: list[dict] = []
    failure_details: list[str] = []
    rule_pass = rule_fail = 0

    for rule_id in rule_ids:
        rule_dir = os.path.join(published_dir, rule_id)
        yml_files = [f for f in os.listdir(rule_dir) if f.endswith(".yml")]
        if not yml_files:
            print(
                f"WARNING: Skipping {rule_id} \u2014 no .yml file found",
                file=sys.stderr,
            )
            continue

        rows, details, passed = _validate_one_rule(
            rule_id, scripts_dir, rules_root, engine_dir, python_cmd
        )
        summary_records.extend(rows)
        failure_details.extend(details)

        if passed:
            rule_pass += 1
            print(f"  \u2192 {rule_id}: PASSED")
        else:
            rule_fail += 1
            print(f"  \u2192 {rule_id}: FAILED")

    _write_reports(summary_records, failure_details, rule_pass, rule_fail, output_dir)
    return rule_fail > 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate all Published rules from cdisc-open-rules."
    )
    parser.add_argument(
        "--rules-root",
        required=True,
        help="Absolute path to the cdisc-open-rules checkout (contains Published/).",
    )
    parser.add_argument(
        "--engine-dir",
        required=True,
        help="Absolute path to the cdisc-rules-engine checkout.",
    )
    parser.add_argument(
        "--python-cmd",
        required=True,
        help="Python executable passed through to run_validation.sh.",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Directory where summary_table.md and detail_report.md are written (default: cwd).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    _args = _parse_args()
    _any_failed = validate_all_rules(
        rules_root=os.path.abspath(_args.rules_root),
        engine_dir=os.path.abspath(_args.engine_dir),
        python_cmd=_args.python_cmd,
        output_dir=os.path.abspath(_args.output_dir),
    )
    sys.exit(1 if _any_failed else 0)
