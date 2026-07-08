import os
import subprocess
import unittest
from conftest import get_python_executable


class TestCoreIssue1487(unittest.TestCase):
    "The updated engine throws an error for unsupported version, which is expected behavior."
    def test_unsupported_sdtmig_5_0_version_returns_metadata_error(self):
        command = [
            f"{get_python_executable()}",
            "-m",
            "core",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "5.0",
            "-d",
            os.path.join("tests", "resources", "CoreIssue1487"),
            "-r",
            "CORE-000354",
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
        )

        error_output = result.stderr + result.stdout

        assert result.returncode != 0, (
            "Expected validation command to fail, but it succeeded."
        )

        assert "LibraryMetadataNotFoundError" in error_output, (
            f"Expected LibraryMetadataNotFoundError, but got:\n{error_output}"
        )

        assert (
            "No library metadata found for standard 'sdtmig' version '5.0'."
            in error_output
        ), (
            "Expected missing library metadata error for SDTMIG 5.0, "
            f"but got:\n{error_output}"
        )