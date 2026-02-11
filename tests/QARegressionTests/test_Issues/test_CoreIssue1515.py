import os
import subprocess

import pytest
import json
from conftest import get_python_executable


@pytest.mark.regression
class TestCoreIssue1515:
    def test_raw_report(self):
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
            "-dv",
            "2-1",
            "-dxp",
            os.path.join("tests", "resources", "CoreIssue1515", "define.xml"),
            "-lr",
            os.path.join("tests", "resources", "CoreIssue1515", "rule.yml"),
            "-vx",
            "y",
            "-d",
            "tests/resources/CoreIssue1515/datasets",
            "--output-format",
            "JSON",
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

        assert json_report.get("Rules_Report")[0].get("status") == "SUCCESS"
        lb_issues = [
            x for x in json_report.get("Issue_Details") if x.get("dataset") == "lb.json"
        ]
        assert len(lb_issues) == 1
        # C102580 is not present in define.xml
        assert lb_issues[0].get("values") == [
            "LBSTRESC",
            "LBSTRESC",
            "C102580",
            "Char",
            "LBSTRESC",
            "null",
        ]
        if os.path.exists(json_report_path):
            os.remove(json_report_path)
