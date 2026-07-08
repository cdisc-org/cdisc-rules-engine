import os
import subprocess
import json

import pytest
from conftest import get_python_executable


class TestCoreIssue1023:

    def test_dataset_utf8(self):
        """Test that the engine correctly handles dataset files with utf8 encoding"""
        command = [
            f"{get_python_executable()}",
            "-m",
            "core",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-r",
            "CORE-000766",
            "-e",
            "utf8",
            "-dp",
            os.path.join("tests", "resources", "CoreIssue1023", "ae_utf8.json"),
            "--output-format",
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
        json_report = json.load(open(json_report_path, "r", encoding="utf8"))
        assert {
            "Conformance_Details",
            "Dataset_Details",
            "Issue_Summary",
            "Issue_Details",
            "Rules_Report",
        }.issubset(json_report.keys())
        datasets = {x["filename"] for x in json_report["Dataset_Details"]}
        assert datasets == {"AE"}
        assert (
            json_report["Issue_Details"][0]["USUBJID"]
            == f"utf8:{b'\xe1\xbc\x87'.decode('utf8')}"
        )
        if os.path.exists(json_report_path):
            os.remove(json_report_path)
