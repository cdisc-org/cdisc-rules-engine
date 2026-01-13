import os
import subprocess
import unittest

import pytest
import json
from conftest import get_python_executable


@pytest.mark.regression
class TestCoreIssue1501(unittest.TestCase):
    def test_raw_report(self):
        # Run the command in the terminal
        command = [
            f"{get_python_executable()}",
            "-m",
            "core",
            "validate",
            "-s",
            "send",
            "-v",
            "1-0",
            "-dp",
            "tests/resources/CoreIssue1501/unit-test-coreid-SENDIG282-negative.json",
            "-lr",
            "tests/resources/CoreIssue1501/Rule.yml",
            "-ps",
            "1",
            "-of",
            "json",
            "-rr",
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
        assert ['Conformance_Details', 'Dataset_Details', 'Issue_Summary', 'Issue_Details', 'Rules_Report'] in json_report

        if not 'results_data' in json_report.keys():
            assert False, "'results_data' key not found in report. Expected while using -rr, --raw_report flag"
        if os.path.exists(json_report_path):
            os.remove(json_report_path)
