import subprocess
import pytest
from conftest import get_python_executable


@pytest.mark.regression
def test_non_existing_dataset_shows_helpful_error():
    """Test that the engine displays a helpful error message if dataset files are not found"""
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
        "ds.json",
        "-dp",
        "ds2.json",
    ]

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    stderr_text = stderr.decode()
    stdout_text = stdout.decode()

    assert process.returncode == 1, "Expected non-zero exit code"
    assert (
        "FileNotFoundError" not in stderr_text
    ), "Should not show FileNotFoundError crash"
    assert (
        "Failed to execute script" not in stderr_text
    ), "Should not show script execution failure"
    assert "Files ds.json, ds2.json are not found" in stderr_text, (
        f"Expected helpful error message about non-existing dataset files. "
        f"stderr: {stderr_text}, stdout: {stdout_text}"
    )
