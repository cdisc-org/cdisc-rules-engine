import os
import subprocess

import pytest
import json
from conftest import get_python_executable


@pytest.mark.regression
class TestCoreIssue984:
    def test_define_subversion_ignored(self):
        # Run the command in the terminal
        command = [
            f"{get_python_executable()}",
            "-m",
            "core",
            "validate",
            "-s",
            "sdtmig",
            "-r",
            "CORE-000007",
            "-v",
            "3.4",
            "-dp",
            os.path.join(
                "tests",
                "resources",
                "test_dataset.json",
            ),
            "-dxp",
            os.path.join("tests", "resources", "CoreIssue984", "define.xml"),
            "--output-format",
            "json",
            "-ps",
            "1",
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

        assert {
            "Conformance_Details",
            "Dataset_Details",
            "Issue_Summary",
            "Issue_Details",
            "Rules_Report",
        }.issubset(json_report.keys())
        assert json_report["Conformance_Details"]["Standard"] == "SDTMIG"
        assert json_report["Conformance_Details"]["Define_XML_Version"] == "2.1.5"

        if os.path.exists(json_report_path):
            os.remove(json_report_path)
