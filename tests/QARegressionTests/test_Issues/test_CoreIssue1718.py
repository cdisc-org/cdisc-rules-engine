import os
import subprocess
import pytest
import json
from conftest import get_python_executable


@pytest.mark.regression
class TestCoreIssue1718:
    def test_max_issues(self):
        # Run the command in the terminal
        max_issues = 3
        command = [
            f"{get_python_executable()}",
            "-m",
            "core",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3-4",
            "-d",
            os.path.join(
                "tests",
                "resources",
                "CoreIssue1718",
            ),
            "-r",
            "CORE-000356",
            "-ps",
            "1",
            "-of",
            "json",
            "-me",
            f"{max_issues}",
            "true",
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
        assert json_report["Rules_Report"][0]["status"] == "ISSUE REPORTED"
        assert json_report["Issue_Summary"][0]["issues"] == 74
        assert len(json_report["Issue_Details"]) == max_issues

        if os.path.exists(json_report_path):
            os.remove(json_report_path)
