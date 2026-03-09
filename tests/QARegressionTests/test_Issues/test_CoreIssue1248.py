import os
import subprocess
import unittest

import pytest
import json
from conftest import get_python_executable


@pytest.mark.regression
class TestCoreIssue1248(unittest.TestCase):
    def test_define_path_used(self):
        command = [
            f"{get_python_executable()}",
            "-m",
            "core",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3-2",
            "-of",
            "JSON",
            "-lr",
            "tests/resources/CoreIssue1248/sample.yml",
            "-cs",
            "-dxp",
            "tests/resources/CoreIssue1248/define_subfolder/define.xml",
            "-dp",
            "tests/resources/CoreIssue1248/relrec.json",
        ]
        subprocess.run(command, check=True)

        # Get the latest created report file
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
        assert len(json_report["Issue_Details"]) == 6
        assert json_report["Rules_Report"][0]["status"] == "ISSUE REPORTED"
        if os.path.exists(json_report_path):
            os.remove(json_report_path)
