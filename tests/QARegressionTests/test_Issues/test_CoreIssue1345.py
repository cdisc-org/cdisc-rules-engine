import os
import subprocess
import unittest

import pytest
import json
from conftest import get_python_executable


@pytest.mark.regression
class TestCoreIssue1501(unittest.TestCase):
    def test_no_duplicates_in_supp_dm(self):
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
            os.path.join(
                "tests",
                "resources",
                "CoreIssue1345",
                "subset",
                "define_msg20_testsupp_core.xml",
            ),
            "-d",
            os.path.join("tests", "resources", "CoreIssue1345", "subset"),
            "-lr",
            os.path.join(
                "tests", "resources", "CoreIssue1345", "subset", "draft_rules"
            ),
            "-r",
            "CDISC.SDTMIG.CG0019",
            "-l",
            "error",
            "-ps",
            "1",
            "-of",
            "json",
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
        dataset_filenames = {
            d["filename"].upper() for d in json_report.get("Dataset_Details", [])
        }

        assert "DM" in dataset_filenames, "DM dataset is missing from Dataset_Details"
        assert (
            "SUPPDM" in dataset_filenames
        ), "SUPPDM dataset is missing from Dataset_Details"

        # 2. check for DM / SUPPDM  Issue_Details
        dm_related_issues = [
            issue
            for issue in json_report.get("Issue_Details", [])
            if issue.get("dataset", "").lower() in {"dm.json", "suppdm.json"}
        ]

        assert not dm_related_issues, (
            "Found issues related to DM/SUPPDM datasets:\n" f"{dm_related_issues}"
        )

        dm_related_summary = [
            s
            for s in json_report.get("Issue_Summary", [])
            if s.get("dataset", "").lower() in {"dm.json", "suppdm.json"}
        ]

        assert not dm_related_summary, (
            "Found issue summary entries related to DM/SUPPDM:\n"
            f"{dm_related_summary}"
        )
