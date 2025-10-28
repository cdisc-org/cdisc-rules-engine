import subprocess
import os
import tempfile
import pytest
from conftest import get_python_executable


@pytest.mark.regression
def test_folder_with_xlsx_files_shows_helpful_error():
    with tempfile.TemporaryDirectory() as temp_dir:
        xlsx_files = ["ae.xlsx", "dm.xlsx"]

        for filename in xlsx_files:
            open(os.path.join(temp_dir, filename), "w").close()

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

        assert process.returncode != 0, "Expected non-zero exit code for invalid files"
        assert "KeyError: 'XLSX'" not in stderr_text, "Should not show KeyError crash"
        assert (
            "Failed to execute script" not in stderr_text
        ), "Should not show script execution failure"
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

        assert process.returncode != 0, "Expected non-zero exit code for empty folder"
        assert (
            "No valid dataset files found" in stderr_text
            or "No valid dataset files found" in stdout_text
        ), "Expected helpful error message for empty folder"


@pytest.mark.regression
def test_valid_xpt_files_work_normally():
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

    assert "KeyError: 'XLSX'" not in stderr_text, "Should not show KeyError"
    assert "Failed to execute script" not in stderr_text, "Should not crash"
    assert (
        "No valid dataset files found" not in stderr_text
    ), "Should find valid XPT file"


@pytest.mark.regression
def test_mixed_folder_processes_valid_files():
    with tempfile.TemporaryDirectory() as temp_dir:
        import shutil

        valid_xpt = os.path.join("tests", "resources", "test_dataset.xpt")
        if os.path.exists(valid_xpt):
            shutil.copy(valid_xpt, os.path.join(temp_dir, "ae.xpt"))

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

            assert "KeyError: 'XLSX'" not in stderr_text, "Should not crash on XLSX"
            assert "Failed to execute script" not in stderr_text, "Should not crash"
            assert (
                "No valid dataset files found" not in stderr_text
                and "No valid dataset files found" not in stdout_text
            ), "Should find the valid XPT file"
