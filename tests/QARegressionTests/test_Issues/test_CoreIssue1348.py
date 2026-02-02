import os
import subprocess

import pytest
import json
from conftest import get_python_executable

_message = (
    "The study version's study phase is not specified according to the extensible Trial Phase Response"
    ' (C66737) SDTM codelist - codeSystem is not "http://www.cdisc.org", codeSystemVersion is not a valid'
    " terminology package date, and/or the code or decode is found in the codelist (case insensitive)"
    " but the corresponding decode or code does not match the codelist value (case sensitive)."
)
_summary = [
    {
        "dataset": "StudyVersion.xpt",
        "core_id": "CORE-000409",
        "message": _message,
        "issues": 7,
    }
]
_issue_details = [
    {
        "core_id": "CORE-000409",
        "message": _message,
        "executability": "fully executable",
        "dataset": "StudyVersion.xpt",
        "USUBJID": "",
        "row": 1,
        "SEQ": "",
        "variables": [
            "$code_for_pref_term",
            "$code_for_value",
            "$pref_term_for_code",
            "$value_for_code",
            "studyPhase.standardCode.code",
            "studyPhase.standardCode.decode",
        ],
        "values": [
            "C199989",
            "C199989",
            "Phase II Trial",
            "PHASE II TRIAL",
            "C15601",
            "Phase Ib Trial",
        ],
    },
    {
        "core_id": "CORE-000409",
        "message": _message,
        "executability": "fully executable",
        "dataset": "StudyVersion.xpt",
        "USUBJID": "",
        "row": 2,
        "SEQ": "",
        "variables": [
            "$code_for_pref_term",
            "$code_for_value",
            "$pref_term_for_code",
            "$value_for_code",
            "studyPhase.standardCode.code",
            "studyPhase.standardCode.decode",
        ],
        "values": ["null", "null", "Not Applicable", "NOT APPLICABLE", "C48660", "N/A"],
    },
    {
        "core_id": "CORE-000409",
        "message": _message,
        "executability": "fully executable",
        "dataset": "StudyVersion.xpt",
        "USUBJID": "",
        "row": 3,
        "SEQ": "",
        "variables": [
            "$code_for_pref_term",
            "$code_for_value",
            "$pref_term_for_code",
            "$value_for_code",
            "studyPhase.standardCode.code",
            "studyPhase.standardCode.decode",
        ],
        "values": [
            "C198366",
            "C198366",
            "null",
            "null",
            "C198366xx",
            "Phase I/II/III Trial",
        ],
    },
    {
        "core_id": "CORE-000409",
        "message": _message,
        "executability": "fully executable",
        "dataset": "StudyVersion.xpt",
        "USUBJID": "",
        "row": 4,
        "SEQ": "",
        "variables": [
            "$code_for_pref_term",
            "$code_for_value",
            "$pref_term_for_code",
            "$value_for_code",
            "studyPhase.standardCode.code",
            "studyPhase.standardCode.decode",
        ],
        "values": ["C15600", "C15600", "null", "null", "C00001x", "Phase I Trial"],
    },
    {
        "core_id": "CORE-000409",
        "message": _message,
        "executability": "fully executable",
        "dataset": "StudyVersion.xpt",
        "USUBJID": "",
        "row": 5,
        "SEQ": "",
        "variables": [
            "$code_for_pref_term",
            "$code_for_value",
            "$pref_term_for_code",
            "$value_for_code",
            "studyPhase.standardCode.code",
            "studyPhase.standardCode.decode",
        ],
        "values": [
            "C198366",
            "C198366",
            "Phase II Trial",
            "PHASE II TRIAL",
            "C15601",
            "Phase I/II/III Trial",
        ],
    },
    {
        "core_id": "CORE-000409",
        "message": _message,
        "executability": "fully executable",
        "dataset": "StudyVersion.xpt",
        "USUBJID": "",
        "row": 6,
        "SEQ": "",
        "variables": [
            "$code_for_pref_term",
            "$code_for_value",
            "$pref_term_for_code",
            "$value_for_code",
            "studyPhase.standardCode.code",
            "studyPhase.standardCode.decode",
        ],
        "values": ["C199989", "C199989", "null", "null", "C198366xx", "PHASE IB TRIAL"],
    },
    {
        "core_id": "CORE-000409",
        "message": _message,
        "executability": "fully executable",
        "dataset": "StudyVersion.xpt",
        "USUBJID": "",
        "row": 7,
        "SEQ": "",
        "variables": [
            "$code_for_pref_term",
            "$code_for_value",
            "$pref_term_for_code",
            "$value_for_code",
            "studyPhase.standardCode.code",
            "studyPhase.standardCode.decode",
        ],
        "values": [
            "null",
            "null",
            "Phase Ib Trial",
            "PHASE IB TRIAL",
            "C199989",
            "PHASE 2 TRIAL",
        ],
    },
]


@pytest.mark.regression
class TestCoreIssue1348:
    @pytest.mark.parametrize(
        "rule_name, dataset, issue_summary, details",
        [
            ("DDF00015_not.yaml", "data_pos", [], []),
            ("DDF00015_not.yaml", "data_neg", _summary, _issue_details),
            ("DDF00015_all.yaml", "data_pos", [], []),
            ("DDF00015_all.yaml", "data_neg", _summary, _issue_details),
        ],
    )
    def test_not_as_first_condition_same_errors_as_all_condition(
        self, rule_name, dataset, issue_summary, details
    ):
        # Run the command in the terminal
        command = [
            f"{get_python_executable()}",
            "-m",
            "core",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3-4",
            "-lr",
            os.path.join("tests", "resources", "CoreIssue1348", rule_name),
            "-of",
            "json",
            "--data",
            os.path.join("tests", "resources", "CoreIssue1348", dataset),
        ]
        subprocess.run(command, check=True)

        # Get the latest created Excel file
        files = os.listdir()
        json_files = [
            file
            for file in files
            if file.startswith("CORE-Report-") and file.endswith(".json")
        ]
        json_report_path = sorted(json_files)[-1]
        # Open the JSON report file
        json_report = json.load(open(json_report_path))
        assert {
            "Conformance_Details",
            "Dataset_Details",
            "Issue_Summary",
            "Issue_Details",
            "Rules_Report",
        }.issubset(json_report.keys())

        assert details == json_report["Issue_Details"]
        assert issue_summary == json_report["Issue_Summary"]
        if os.path.exists(json_report_path):
            os.remove(json_report_path)
