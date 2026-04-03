import os
import subprocess
import unittest

import pytest
import json
from conftest import get_python_executable


@pytest.mark.regression
class TestCoreIssue1558(unittest.TestCase):
    def test_raw_report(self):
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
            "-d",
            os.path.join(
                "tests",
                "resources",
                "CoreIssue1558",
                "datasets",
            ),
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
        datasets = {x["filename"] for x in json_report["Dataset_Details"]}
        assert {"LB", "DM"}.issubset(datasets)
        if os.path.exists(json_report_path):
            os.remove(json_report_path)

    def test_env_vars_loaded(self):
        command = [
            f"{get_python_executable()}",
            "-m",
            "core",
            "validate",
            "-r",
            "CORE-000007",
            "--dotenv-path",
            os.path.join("tests", "resources", "CoreIssue1558", "test.env"),
            "-d",
            os.path.join(
                "tests",
                "resources",
                "CoreIssue1558",
                "datasets",
            ),
            "--output-format",
            "json",
            "-ps",
            "1",
        ]
        subprocess.run(command, capture_output=True, text=True)
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
        assert json_report["Conformance_Details"]["Standard"] == "SDTMIG"
        assert json_report["Conformance_Details"]["Version"] == "V3.4"
