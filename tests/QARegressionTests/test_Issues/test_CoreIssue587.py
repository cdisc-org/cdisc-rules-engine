import json
import os
import subprocess

import pytest
from conftest import get_python_executable


def _latest_json_report(before_files: set[str]) -> str:
    files = set(os.listdir())
    new_json = sorted(
        [
            f
            for f in (files - before_files)
            if f.startswith("CORE-Report-") and f.endswith(".json")
        ]
    )
    if new_json:
        return new_json[-1]
    # Fallback if report file already existed from prior runs
    all_json = sorted(
        [f for f in files if f.startswith("CORE-Report-") and f.endswith(".json")]
    )
    assert all_json, "No CORE JSON report was produced"
    return all_json[-1]


def _issue_row_text(issue_row: dict) -> str:
    # Make a resilient searchable blob regardless of key naming
    return " | ".join(str(v) for v in issue_row.values() if v is not None)


@pytest.mark.regression
@pytest.mark.parametrize(
    "case_folder,expected_missing_trtxxa",
    [
        ("paired_only", []),
        ("single_missing", ["TRT01A"]),
        ("mixed_partial", ["TRT01A"]),
        ("multiple_missing", ["TRT01A", "TRT02A"]),
        ("boundary_99_missing", ["TRT99A"]),
        ("boundary_99_paired", []),
        ("nonmatching_noise", []),
    ],
)
def test_coreissue587_adam64(case_folder, expected_missing_trtxxa):
    # Expected folder structure:
    # tests/resources/CoreIssue587/<case_folder>/Dataset.json
    # tests/resources/CoreIssue587/<case_folder>/Rule.yml

    case_root = os.path.join("tests", "resources", "CoreIssue587", case_folder)
    dataset_path = os.path.join(case_root, "Dataset.json")
    rule_path = os.path.join("tests", "resources", "CoreIssue587", "Rule.yml")

    assert os.path.exists(dataset_path), f"Missing dataset file: {dataset_path}"
    assert os.path.exists(rule_path), f"Missing rule file: {rule_path}"

    command = [
        f"{get_python_executable()}",
        "-m",
        "core",
        "validate",
        "-s",
        "adamig",
        "-v",
        "1-3",
        "-dp",
        dataset_path,
        "-lr",
        rule_path,
        "-ps",
        "1",
        "-of",
        "json",
    ]

    before_files = set(os.listdir())
    subprocess.run(command, check=True)

    json_report_path = _latest_json_report(before_files)
    try:
        with open(json_report_path, encoding="utf-8") as f:
            json_report = json.load(f)

        assert {
            "Conformance_Details",
            "Dataset_Details",
            "Issue_Summary",
            "Issue_Details",
            "Rules_Report",
        }.issubset(json_report.keys())

        issue_details = json_report.get("Issue_Details", [])
        rules_report = json_report.get("Rules_Report", [])
        assert rules_report, "Rules_Report should contain at least one row"

        # Core count expectation for this rule
        assert len(issue_details) == len(expected_missing_trtxxa), (
            f"Expected {len(expected_missing_trtxxa)} issues, got {len(issue_details)}. "
            f"Issues: {issue_details}"
        )

        # Status expectation
        expected_status = "ISSUE REPORTED" if expected_missing_trtxxa else "SUCCESS"
        assert rules_report[0].get("status") == expected_status

        # Optional stronger assertion:
        # ensure each expected missing TRTxxA token appears in at least one issue row payload
        if expected_missing_trtxxa:
            issue_payload = "\n".join(_issue_row_text(row) for row in issue_details)
            for token in expected_missing_trtxxa:
                assert (
                    token in issue_payload
                ), f"Expected token {token} not found in issue payload:\n{issue_payload}"

    finally:
        if os.path.exists(json_report_path):
            os.remove(json_report_path)
