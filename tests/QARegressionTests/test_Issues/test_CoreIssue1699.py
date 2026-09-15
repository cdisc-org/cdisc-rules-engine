import os
import subprocess
import unittest
import json
from conftest import get_python_executable


class TestCoreIssue1699(unittest.TestCase):
    def test_cg0314_supp_rule_reports_success(self):
        """Validates the CG0314 SUPP (Supplemental Qualifiers) rule via
        rule.yml against the CoreIssue1699 dataset, asserting the rule
        reports a SUCCESS status.
        """
        # Run the command in the terminal
        command = [
            f"{get_python_executable()}",
            "-m",
            "core",
            "validate",
            "-s",
            "sendig",
            "-v",
            "3-1",
            "-d",
            os.path.join(
                "tests",
                "resources",
                "CoreIssue1699",
            ),
            "-lr",
            os.path.join("tests", "resources", "CoreIssue1699", "rule.yml"),
            "-ps",
            "1",
            "-of",
            "json",
        ]
        subprocess.run(command, check=True)

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
        assert json_report["Rules_Report"][0]["status"] == "SUCCESS"

        if os.path.exists(json_report_path):
            os.remove(json_report_path)
