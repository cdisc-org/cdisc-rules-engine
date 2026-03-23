import os
import subprocess

import pytest
import json
from conftest import get_python_executable


@pytest.mark.regression
class TestCoreIssue1248:
    @pytest.mark.parametrize(
        "command,rules_report,num_issues",
        [
            # define path provided will lead to successful execution
            (
                [
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
                    os.path.join("tests", "resources", "CoreIssue1248", "sample.yml"),
                    "-cs",
                    "-dxp",
                    os.path.join(
                        "tests",
                        "resources",
                        "CoreIssue1248",
                        "define_subfolder",
                        "define.xml",
                    ),
                    "-ps",
                    "1",
                    "-dp",
                    os.path.join("tests", "resources", "CoreIssue1248", "data.xlsx"),
                ],
                [
                    {
                        "core_id": "SD1129",
                        "version": "1",
                        "cdisc_rule_id": "",
                        "fda_rule_id": "",
                        "message": "TEST",
                        "status": "ISSUE REPORTED",
                    }
                ],
                2,
            ),
            # JSON data file and no define.xml in same folder and no -dxp param will provide error
            (
                [
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
                    os.path.join("tests", "resources", "CoreIssue1248", "sample.yml"),
                    "-cs",
                    "-dp",
                    os.path.join("tests", "resources", "CoreIssue1248", "relrec.json"),
                    "-ps",
                    "1",
                ],
                [
                    {
                        "core_id": "SD1129",
                        "version": "1",
                        "cdisc_rule_id": "",
                        "fda_rule_id": "",
                        "message": "TEST",
                        "status": "EXECUTION ERROR",
                    }
                ],
                1,
            ),
            # no define.xml in same path as data.xlsx file will provide error
            (
                [
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
                    os.path.join("tests", "resources", "CoreIssue1248", "sample.yml"),
                    "-cs",
                    "-dp",
                    os.path.join("tests", "resources", "CoreIssue1248", "data.xlsx"),
                    "-ps",
                    "1",
                ],
                [
                    {
                        "core_id": "SD1129",
                        "version": "1",
                        "cdisc_rule_id": "",
                        "fda_rule_id": "",
                        "message": "TEST",
                        "status": "EXECUTION ERROR",
                    }
                ],
                1,
            ),
            # define.xml in same folder as the data.xls and no -dxp provided will provide error until
            # in ExcelDataService dataset metadata creation full_path=dataset_name
            (
                [
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
                    os.path.join("tests", "resources", "CoreIssue1248", "sample.yml"),
                    "-cs",
                    "-dp",
                    os.path.join(
                        "tests",
                        "resources",
                        "CoreIssue1248",
                        "data_and_define",
                        "data.xlsx",
                    ),
                    "-ps",
                    "1",
                ],
                [
                    {
                        "core_id": "SD1129",
                        "version": "1",
                        "cdisc_rule_id": "",
                        "fda_rule_id": "",
                        "message": "TEST",
                        "status": "EXECUTION ERROR",
                    }
                ],
                1,
            ),
        ],
    )
    def test_define_path_used(self, command, rules_report, num_issues):
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
        assert len(json_report["Issue_Details"]) == num_issues
        assert json_report["Rules_Report"] == rules_report
        if os.path.exists(json_report_path):
            os.remove(json_report_path)
