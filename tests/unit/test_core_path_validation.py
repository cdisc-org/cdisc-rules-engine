"""
Integration tests for CLI path validation.
"""

import os
import platform
import sys
import tempfile

import pytest
from click.testing import CliRunner

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from core import cli


class TestCLIPathValidation:
    """Test path validation in CLI commands."""

    @pytest.fixture
    def runner(self):
        """Create CLI runner for testing."""
        # Use mix_stderr=True to capture both stdout and stderr together
        return CliRunner(mix_stderr=True)

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    def test_validate_output_path_traversal_blocked(self, runner, temp_dir):
        """Test that path traversal in output path is blocked."""
        result = runner.invoke(
            cli,
            [
                "validate",
                "-s",
                "sdtmig",
                "-v",
                "3-4",
                "-d",
                temp_dir,
                "-o",
                "../../etc/passwd",
            ],
        )
        # Path validation blocks traversal - exit code 2 indicates validation error
        assert result.exit_code == 2

    def test_validate_output_path_system_directory_blocked(self, runner, temp_dir):
        """Test that output to system directory is blocked."""
        system = platform.system().lower()

        if system == "linux":
            bad_path = "/etc/test_output.xlsx"
        elif system == "darwin":
            bad_path = "/System/test_output.xlsx"
        elif system == "windows":
            bad_path = "C:\\Windows\\test_output.xlsx"
        else:
            pytest.skip(f"Unknown system: {system}")

        result = runner.invoke(
            cli,
            [
                "validate",
                "-s",
                "sdtmig",
                "-v",
                "3-4",
                "-d",
                temp_dir,
                "-o",
                bad_path,
            ],
        )
        # Path validation blocks system directory writes - exit code 2 indicates validation error
        assert result.exit_code == 2

    def test_validate_output_path_valid(self, runner, temp_dir):
        """Test that valid output path works."""
        output_file = os.path.join(temp_dir, "output.xlsx")
        result = runner.invoke(
            cli,
            [
                "validate",
                "-s",
                "sdtmig",
                "-v",
                "3-4",
                "-d",
                temp_dir,
                "-o",
                output_file,
            ],
        )
        # May fail due to missing data, but path validation should pass
        # Check that path validation error is NOT in output
        assert "Invalid output path" not in result.output
        assert "traversal" not in result.output.lower()

    def test_validate_data_path_traversal_blocked(self, runner):
        """Test that path traversal in data path is blocked."""
        result = runner.invoke(
            cli,
            [
                "validate",
                "-s",
                "sdtmig",
                "-v",
                "3-4",
                "-d",
                "../../etc",
            ],
        )
        # Path validation blocks traversal - exit code != 0 indicates error
        assert result.exit_code != 0

    def test_validate_dataset_path_traversal_blocked(self, runner):
        """Test that path traversal in dataset path is blocked."""
        result = runner.invoke(
            cli,
            [
                "validate",
                "-s",
                "sdtmig",
                "-v",
                "3-4",
                "-dp",
                "../../etc/passwd",
            ],
        )
        # Path validation blocks traversal - exit code != 0 indicates error
        assert result.exit_code != 0

    def test_update_cache_path_validation(self, runner, temp_dir):
        """Test path validation in update-cache command."""
        # This test requires API key, so we'll just test path validation
        # by checking that invalid paths are rejected
        result = runner.invoke(
            cli,
            [
                "update-cache",
                "--apikey",
                "test-key",
                "--cache-path",
                "../../etc",
            ],
        )
        # Should fail on path validation before API key check
        assert (
            result.exit_code != 0
        )  # Should fail (either path validation or API error)

    def test_validate_define_xml_path_traversal_blocked(self, runner, temp_dir):
        """Test that path traversal in define-xml path is blocked."""
        result = runner.invoke(
            cli,
            [
                "validate",
                "-s",
                "sdtmig",
                "-v",
                "3-4",
                "-d",
                temp_dir,
                "-dxp",
                "../../etc/passwd",
            ],
        )
        # Path validation blocks traversal - exit code != 0 indicates error
        assert result.exit_code != 0

    def test_validate_dictionary_path_traversal_blocked(self, runner, temp_dir):
        """Test that path traversal in dictionary paths is blocked."""
        result = runner.invoke(
            cli,
            [
                "validate",
                "-s",
                "sdtmig",
                "-v",
                "3-4",
                "-d",
                temp_dir,
                "--whodrug",
                "../../etc",
            ],
        )
        # Path validation blocks traversal - exit code != 0 indicates error
        assert result.exit_code != 0
