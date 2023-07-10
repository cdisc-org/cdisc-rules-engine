import unittest
import subprocess
import os
import pytest

""""This issue acceptance criteria depends upin successfull execution of unit tests
created by @nhaydel for the issue"""


@pytest.mark.skip
class TestCoreIssue116(unittest.TestCase):
    def test_label_referenced_varaible_metadata(self):
        unit_test_path = os.path.join(
            "tests",
            "unit",
            "test_operations",
            "test_label_referenced_variable_metadata.py",
        )
        command = f"python -m pytest {unit_test_path}"
        result = subprocess.run(command, shell=True, capture_output=True, text=True)

        self.assertTrue(
            result.returncode == 0,
            f"unit test failed Following is the error : {result.stderr}",
        )

    def test_name_referenced_varaible_metadata(self):
        unit_test_path = os.path.join(
            "tests",
            "unit",
            "test_operations",
            "test_name_referenced_variable_metadata.py",
        )
        command = f"python -m pytest {unit_test_path}"
        result = subprocess.run(command, shell=True, capture_output=True, text=True)

        self.assertTrue(
            result.returncode == 0,
            f"unit test failed Following is the error : {result.stderr}",
        )


if __name__ == "__main__":
    unittest.main()
