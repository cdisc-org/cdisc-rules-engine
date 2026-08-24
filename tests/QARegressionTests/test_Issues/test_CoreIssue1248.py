import os
import subprocess

import pytest
import json
from conftest import get_python_executable


class TestCoreIssue1248:
    @pytest.mark.parametrize(
        "command,rules_report,num_issues",
        [
            # Case 1: -dxp explicitly points to the Define-XML file, even though
            # it lives in a subfolder unrelated to -dp -> validation succeeds.
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
            # Case 2: JSON data with no adjacent define.xml and no -dxp given ->
            # engine cannot locate a Define-XML and reports an execution error.
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
            # Case 3: xlsx data with no define.xml alongside it and no -dxp
            # given -> same as case 2, execution error for a different format.
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
            # Case 4: define.xml sits in the same folder as data.xlsx but -dxp
            # is still not given -> still an execution error (known limitation:
            # ExcelDataService dataset metadata creation uses
            # full_path=dataset_name, so it does not auto-discover a sibling
            # define.xml the way -dp with a JSON/xlsx path might elsewhere).
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
        """Verify how the engine resolves the Define-XML path (-dxp) relative
        to the data path (-dp) across the four scenarios documented above."""
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
