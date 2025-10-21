import subprocess
import os
import tempfile
import pytest
from conftest import get_python_executable

"""
Test for Issue #1387: The error message shown when the -d switch points
to a folder without datasets is not user-friendly.

These tests validate that:
1. Folders with only unsupported formats (XLSX) show helpful error messages
2. Empty folders show helpful error messages
3. Valid XPT files continue to work normally
4. Mixed folders (valid + invalid) process valid files with warnings
"""


@pytest.mark.regression
def test_folder_with_xlsx_files_shows_helpful_error():
    """Test that a folder with only XLSX files shows a user-friendly error message"""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create dummy XLSX files
        xlsx_file1 = os.path.join(temp_dir, "ae.xlsx")
        xlsx_file2 = os.path.join(temp_dir, "dm.xlsx")

        # Create empty XLSX files
        open(xlsx_file1, "w").close()
        open(xlsx_file2, "w").close()

        command = [
            get_python_executable(),
            "-m",
            "core",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-d",
            temp_dir,
        ]

        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()

        stderr_text = stderr.decode()
        stdout_text = stdout.decode()

        # Should exit with error code
        assert process.returncode != 0, "Expected non-zero exit code for invalid files"

        # Should NOT contain the old KeyError crash
        assert "KeyError: 'XLSX'" not in stderr_text, "Should not show KeyError crash"
        assert (
            "Failed to execute script" not in stderr_text
        ), "Should not show script execution failure"

        # Should contain helpful error message
        assert (
            "No valid dataset files found" in stderr_text
            or "No valid dataset files found" in stdout_text
        ), f"Expected helpful error message. stderr: {stderr_text}, stdout: {stdout_text}"
        assert (
            "SAS V5 XPT or Dataset-JSON" in stderr_text
            or "SAS V5 XPT or Dataset-JSON" in stdout_text
        ), "Expected format guidance in error message"


@pytest.mark.regression
def test_empty_folder_shows_helpful_error():
    """Test that an empty folder shows a user-friendly error message"""
    with tempfile.TemporaryDirectory() as temp_dir:
        command = [
            get_python_executable(),
            "-m",
            "core",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-d",
            temp_dir,
        ]

        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()

        stderr_text = stderr.decode()
        stdout_text = stdout.decode()

        # Should exit with error code
        assert process.returncode != 0, "Expected non-zero exit code for empty folder"

        # Should contain helpful error message
        assert (
            "No valid dataset files found" in stderr_text
            or "No valid dataset files found" in stdout_text
        ), "Expected helpful error message for empty folder"


@pytest.mark.regression
def test_valid_xpt_files_work_normally():
    """Test that folders with valid XPT files continue to work normally"""
    command = [
        get_python_executable(),
        "-m",
        "core",
        "validate",
        "-s",
        "sdtmig",
        "-v",
        "3.4",
        "-dp",
        os.path.join("tests", "resources", "test_dataset.xpt"),
    ]

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    stderr_text = stderr.decode()

    # Should succeed or fail validation (but not crash with format error)
    assert "KeyError: 'XLSX'" not in stderr_text, "Should not show KeyError"
    assert "Failed to execute script" not in stderr_text, "Should not crash"
    assert (
        "No valid dataset files found" not in stderr_text
    ), "Should find valid XPT file"


@pytest.mark.regression
def test_mixed_folder_processes_valid_files():
    """Test that a folder with both valid and invalid files processes the valid ones"""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Copy a valid test XPT file to temp directory
        import shutil

        valid_xpt = os.path.join("tests", "resources", "test_dataset.xpt")
        if os.path.exists(valid_xpt):
            shutil.copy(valid_xpt, os.path.join(temp_dir, "ae.xpt"))

            # Create an invalid XLSX file
            xlsx_file = os.path.join(temp_dir, "dm.xlsx")
            open(xlsx_file, "w").close()

            command = [
                get_python_executable(),
                "-m",
                "core",
                "validate",
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                "-d",
                temp_dir,
            ]

            process = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            stdout, stderr = process.communicate()

            stderr_text = stderr.decode()
            stdout_text = stdout.decode()

            # Should process successfully (found the XPT file)
            assert "KeyError: 'XLSX'" not in stderr_text, "Should not crash on XLSX"
            assert "Failed to execute script" not in stderr_text, "Should not crash"

            # May contain warning about ignored files (optional - our enhancement)
            # But should NOT fail completely
            assert (
                "No valid dataset files found" not in stderr_text
                and "No valid dataset files found" not in stdout_text
            ), "Should find the valid XPT file"
