import os
import subprocess
import unittest

import pytest
import json
from conftest import get_python_executable


@pytest.mark.regression
class TestCoreIssue1316(unittest.TestCase):
    def test_new_rule(self):
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
            "-dp",
            os.path.join(
                "tests",
                "resources",
                "CoreIssue1316",
                "test_dataset.json",
            ),
            "-lr",
            os.path.join("tests", "resources", "CoreIssue1316", "rule_new.yml"),
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
        json_report = json.load(open(json_report_path))
        assert {
            "Conformance_Details",
            "Dataset_Details",
            "Issue_Summary",
            "Issue_Details",
            "Rules_Report",
        }.issubset(json_report.keys())
        assert json_report["Issue_Summary"][0]["issues"] == 591
        assert json_report["Rules_Report"][0]["status"] == "ISSUE REPORTED"

        if os.path.exists(json_report_path):
            os.remove(json_report_path)
