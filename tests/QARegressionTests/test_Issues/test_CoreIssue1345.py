import os
import subprocess

import pytest
import json
from conftest import get_python_executable


@pytest.fixture
def generate_report():
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
            "define_msg20_testsupp_core.xml",
        ),
        "-d",
        os.path.join(
            "tests",
            "resources",
            "CoreIssue1345",
        ),
        "-lr",
        os.path.join(
            "tests",
            "resources",
            "CoreIssue1345",
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
    json_report = json.load(open(json_report_path))
    return json_report_path, json_report


@pytest.mark.regression
class TestCoreIssue1345:
    def test_engine_correctly_merges_datasets_and_flags_row_uniqueness_issues(
        self, generate_report
    ):
        json_report_path, json_report = generate_report
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

        ec_detail_issues = [
            i
            for i in json_report.get("Issue_Details", [])
            if i.get("dataset", "").lower() == "ec.json"
        ]

        assert (
            ec_detail_issues
        ), "Expected EC-related issues in Issue_Details, but none found"
        assert (
            len(ec_detail_issues) == 2
        ), f"Expected 2 issues for EC dataset, but {len(ec_detail_issues)} found in Issue_Details"

        ec_summary_issues = [
            s
            for s in json_report.get("Issue_Summary", [])
            if s.get("dataset", "").lower() == "ec.json"
        ]

        assert (
            ec_summary_issues
        ), "Expected issues for EC dataset, but none found in Issue_Summary"

        if os.path.exists(json_report_path):
            os.remove(json_report_path)

    def test_engine_correctly_processes_relrec_when_supp_datasets_provided(
        self, generate_report
    ):
        json_report_path, json_report = generate_report
        # Open the JSON report file
        dataset_filenames = {
            d["filename"].upper() for d in json_report.get("Dataset_Details", [])
        }

        assert "DM" in dataset_filenames, "DM dataset is missing from Dataset_Details"
        assert (
            "SUPPDM" in dataset_filenames
        ), "SUPPDM dataset is missing from Dataset_Details"
        assert "EC" in dataset_filenames, "EC dataset is missing from Dataset_Details"
        assert (
            "SUPPEC" in dataset_filenames
        ), "SUPPEC dataset is missing from Dataset_Details"

        # check that relrec was processed and rule checked the data
        assert (
            "RELREC" in dataset_filenames
        ), "RELREC dataset is missing from Dataset_Details"
        relrec_issues = [
            i
            for i in json_report.get("Issue_Details", [])
            if i.get("dataset", "").lower() == "relrec.json"
        ]
        assert (
            len(relrec_issues) == 2
        ), f"Expected 2 issues for RELREC dataset, but {len(relrec_issues)} found"

        # to confirm that EC is still processed and contains issues
        ec_detail_issues = [
            i
            for i in json_report.get("Issue_Details", [])
            if i.get("dataset", "").lower() == "ec.json"
        ]
        assert (
            len(ec_detail_issues) == 2
        ), f"Expected 2 issues for EC dataset, but {len(ec_detail_issues)} found in Issue_Details"

        if os.path.exists(json_report_path):
            os.remove(json_report_path)
