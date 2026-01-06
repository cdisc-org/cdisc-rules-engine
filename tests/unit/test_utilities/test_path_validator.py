"""
Unit tests for path validator utility.
"""

import os
import platform
from pathlib import Path

import pytest

from cdisc_rules_engine.exceptions.path_validation_exceptions import (
    InvalidPathError,
    PathOutsideAllowedDirectoryError,
    PathTraversalError,
    SystemDirectoryError,
)
from cdisc_rules_engine.utilities.path_validator import PathValidator


class TestPathValidator:
    """Test cases for PathValidator class."""

    def test_validate_read_path_valid_absolute(self, tmp_path):
        """Test validating a valid absolute path for reading."""
        validator = PathValidator()
        test_file = tmp_path / "test.txt"
        test_file.write_text("test")

        result = validator.validate_read_path(str(test_file))
        assert result == test_file.resolve()
        assert result.exists()

    def test_validate_read_path_valid_relative(self, tmp_path):
        """Test validating a valid relative path for reading."""
        validator = PathValidator(allow_relative_paths=True)
        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            test_file = tmp_path / "test.txt"
            test_file.write_text("test")

            result = validator.validate_read_path("test.txt")
            assert result == test_file.resolve()
        finally:
            os.chdir(original_cwd)

    def test_validate_read_path_traversal_detected(self):
        """Test that path traversal is detected."""
        validator = PathValidator(allow_relative_paths=False)

        with pytest.raises(PathTraversalError) as exc_info:
            validator.validate_read_path("../../etc/passwd")

        assert (
            "relative paths" in str(exc_info.value).lower()
            or "traversal" in str(exc_info.value).lower()
        )

    def test_validate_read_path_traversal_allowed_with_warning(self, tmp_path, caplog):
        """Test that traversal is allowed but logged when allow_relative_paths=True."""
        validator = PathValidator(allow_relative_paths=True)
        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            parent_dir = tmp_path.parent
            test_file = parent_dir / "test.txt"
            test_file.write_text("test")

            result = validator.validate_read_path("../test.txt")
            assert result == test_file.resolve()
            assert "traversal pattern" in caplog.text.lower()
        finally:
            os.chdir(original_cwd)

    def test_validate_read_path_empty(self):
        """Test that empty path raises error."""
        validator = PathValidator()

        with pytest.raises(InvalidPathError) as exc_info:
            validator.validate_read_path("")

        assert "empty" in str(exc_info.value).lower()

    def test_validate_read_path_none(self):
        """Test that None path raises error."""
        validator = PathValidator()

        with pytest.raises(InvalidPathError):
            validator.validate_read_path(None)

    def test_validate_write_path_valid(self, tmp_path):
        """Test validating a valid path for writing."""
        validator = PathValidator(block_system_dirs=True)
        output_file = tmp_path / "output.txt"

        result = validator.validate_write_path(str(output_file))
        assert result == output_file.resolve()
        # Parent directory should be created
        assert result.parent.exists()

    def test_validate_write_path_system_directory_blocked(self):
        """Test that writing to system directory is blocked."""
        validator = PathValidator(block_system_dirs=True)
        system = platform.system().lower()

        if system == "linux":
            test_path = "/etc/test.txt"
        elif system == "darwin":
            test_path = "/System/test.txt"
        elif system == "windows":
            test_path = "C:\\Windows\\test.txt"
        else:
            pytest.skip(f"Unknown system: {system}")

        with pytest.raises(SystemDirectoryError) as exc_info:
            validator.validate_write_path(test_path)

        assert "system directory" in str(exc_info.value).lower()

    def test_validate_write_path_within_base_dir(self, tmp_path):
        """Test that path must be within base directory."""
        validator = PathValidator()
        base_dir = tmp_path / "base"
        base_dir.mkdir()
        output_file = tmp_path / "output.txt"  # Outside base_dir

        with pytest.raises(PathOutsideAllowedDirectoryError) as exc_info:
            validator.validate_write_path(str(output_file), base_dir=str(base_dir))

        assert "within" in str(exc_info.value).lower()

    def test_validate_write_path_within_base_dir_valid(self, tmp_path):
        """Test valid path within base directory."""
        validator = PathValidator()
        base_dir = tmp_path / "base"
        base_dir.mkdir()
        output_file = base_dir / "output.txt"  # Inside base_dir

        result = validator.validate_write_path(str(output_file), base_dir=str(base_dir))
        assert result == output_file.resolve()

    def test_is_system_directory_linux(self, monkeypatch):
        """Test system directory detection on Linux."""
        monkeypatch.setattr(platform, "system", lambda: "Linux")
        validator = PathValidator()

        assert validator._is_system_directory(Path("/etc/passwd"))
        assert validator._is_system_directory(Path("/usr/bin/test"))
        assert not validator._is_system_directory(Path("/tmp/test"))
        assert not validator._is_system_directory(Path("/home/user/test"))

    def test_is_system_directory_darwin(self, monkeypatch):
        """Test system directory detection on macOS."""
        monkeypatch.setattr(platform, "system", lambda: "Darwin")
        validator = PathValidator()

        assert validator._is_system_directory(Path("/System/Library"))
        assert validator._is_system_directory(Path("/usr/bin/test"))
        # /tmp and /private/var are allowed (temp directories)
        assert not validator._is_system_directory(Path("/tmp/test"))
        assert not validator._is_system_directory(Path("/private/var/folders/test"))
        assert not validator._is_system_directory(Path("/Users/user/test"))

    def test_is_system_directory_windows(self, monkeypatch):
        """Test system directory detection on Windows."""
        monkeypatch.setattr(platform, "system", lambda: "Windows")
        validator = PathValidator()

        # Test Windows path detection using string paths
        # On Unix, Path() converts Windows paths, so we test the string logic directly
        # Path is already imported at top of file

        # Create a mock path that represents a Windows path
        # The _is_system_directory method handles string normalization
        test_path = Path("C:/Windows/System32")
        assert validator._is_system_directory(test_path)

        # Test non-system directories
        assert not validator._is_system_directory(Path("C:/Users/test"))
        assert not validator._is_system_directory(Path("C:/temp/test"))

    def test_check_user_permissions_regular_user(self, monkeypatch):
        """Test permission check for regular user."""
        if platform.system().lower() != "windows":
            if hasattr(os, "geteuid"):
                monkeypatch.setattr(os, "geteuid", lambda: 1000)  # Non-root

        permissions = PathValidator.check_user_permissions()
        assert "is_admin" in permissions
        assert "username" in permissions
        assert "system" in permissions
        assert "recommendation" in permissions

    def test_whitelist_validation(self, tmp_path):
        """Test whitelist validation."""
        allowed_dir = tmp_path / "allowed"
        allowed_dir.mkdir()
        disallowed_dir = tmp_path / "disallowed"
        disallowed_dir.mkdir()

        validator = PathValidator(
            allowed_base_dirs=[str(allowed_dir)], allow_relative_paths=True
        )

        # Path within allowed directory should work
        test_file = allowed_dir / "test.txt"
        test_file.write_text("test")
        result = validator.validate_read_path(str(test_file))
        assert result == test_file.resolve()

        # Path outside allowed directory should fail
        test_file2 = disallowed_dir / "test.txt"
        test_file2.write_text("test")
        with pytest.raises(PathOutsideAllowedDirectoryError):
            validator.validate_read_path(str(test_file2))

    def test_resolve_path_handles_symlinks(self, tmp_path):
        """Test that path resolution handles symlinks."""
        validator = PathValidator()
        target = tmp_path / "target.txt"
        target.write_text("target")
        symlink = tmp_path / "symlink.txt"
        try:
            symlink.symlink_to(target)
            result = validator._resolve_path(str(symlink))
            assert result == target.resolve()
        except (OSError, NotImplementedError):
            # Symlinks not supported on this platform
            pytest.skip("Symlinks not supported on this platform")

    def test_resolve_path_nonexistent(self):
        """Test resolving a non-existent path."""
        validator = PathValidator()
        # Should not raise error for non-existent paths
        result = validator._resolve_path("/nonexistent/path/file.txt")
        assert isinstance(result, Path)

    def test_is_within_base_python39_plus(self, tmp_path):
        """Test _is_within_base method."""
        validator = PathValidator()
        base = tmp_path / "base"
        base.mkdir()
        subdir = base / "subdir"
        subdir.mkdir()
        outside = tmp_path / "outside"

        assert validator._is_within_base(subdir, base) is True
        assert validator._is_within_base(outside, base) is False
        assert validator._is_within_base(base, base) is True
