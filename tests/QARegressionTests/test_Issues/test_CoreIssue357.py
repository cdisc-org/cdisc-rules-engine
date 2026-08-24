import os
import subprocess
import unittest
from conftest import get_python_executable


class TerminalCommandTestCase(unittest.TestCase):
    """Regression test for SEND rule 266 (variable not allowed in SDTM/SEND
    model) run against a negative dataset.

    Note: this only asserts that a report file is produced, it does not check
    the rule's actual findings/issue content.
    """

    @classmethod
    def setUpClass(cls):
        # Run the command in the terminal
        command = [
            f"{get_python_executable()}",
            "-m",
            "core",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-lr",
            "tests/resources/CoreIssue357/SENDIG_266_rule.json",
            "-dp",
            "tests/resources/CoreIssue357/SENDIG_266_negative_testdata_datasets.json",
        ]
        subprocess.run(command, check=True)

        # Get the latest created Excel file
        files = os.listdir()
        excel_files = [
            file
            for file in files
            if file.startswith("CORE-Report-") and file.endswith(".xlsx")
        ]
        cls.excel_file_path = sorted(excel_files)[-1]

    def test_command_execution(self):
        """Test that validating against the local SEND-266 rule produces an
        Excel report file."""
        # Check if the Excel file is created
        self.assertTrue(
            os.path.exists(self.excel_file_path),
            f"Excel file '{self.excel_file_path}' is not created.",
        )

    @classmethod
    def tearDownClass(cls):
        # Delete the Excel file
        if os.path.exists(cls.excel_file_path):
            os.remove(cls.excel_file_path)


if __name__ == "__main__":
    unittest.main()
