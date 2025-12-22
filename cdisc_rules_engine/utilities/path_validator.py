"""
Path validation utility for security hardening.

This module provides path validation to prevent:
- Path traversal attacks
- Access to system directories
- Unauthorized file access
"""

import os
import platform
import logging
from pathlib import Path
from typing import Optional, List

from cdisc_rules_engine.exceptions.path_validation_exceptions import (
    PathTraversalError,
    SystemDirectoryError,
    PathOutsideAllowedDirectoryError,
    InvalidPathError,
)

logger = logging.getLogger(__name__)


class PathValidator:
    """
    Validates and sanitizes file paths for security.
    
    This class provides methods to validate paths for reading and writing
    operations, preventing path traversal attacks and blocking access to
    system directories.
    """
    
    # System directories that should never be written to
    # Note: /private on macOS is used for temp files, so we exclude /private/var
    SYSTEM_DIRECTORIES: dict[str, List[str]] = {
        "linux": [
            "/bin", "/boot", "/dev", "/etc", "/lib", "/lib64",
            "/proc", "/root", "/run", "/sbin", "/sys", "/usr",
            "/var/log", "/var/run", "/var/lib", "/opt",
        ],
        "darwin": [  # macOS
            "/Applications", "/Library", "/System", "/bin", "/sbin",
            "/usr", "/private/etc", "/private/var/log", "/private/var/run",
            "/private/var/lib", "/System/Library",
        ],
        "windows": [
            "C:\\Windows", "C:\\Program Files", "C:\\Program Files (x86)",
            "C:\\ProgramData", "C:\\System32", "C:\\SysWOW64",
            "C:\\Windows\\System32", "C:\\Windows\\SysWOW64",
        ],
    }
    
    def __init__(
        self,
        block_system_dirs: bool = True,
        allowed_base_dirs: Optional[List[str]] = None,
        allow_relative_paths: bool = True,
    ):
        """
        Initialize path validator.
        
        Args:
            block_system_dirs: If True, block access to system directories
            allowed_base_dirs: Optional list of allowed base directories
                              (whitelist). If provided, paths must be within
                              one of these directories.
            allow_relative_paths: If True, allow relative paths (default: True)
        """
        self.block_system_dirs = block_system_dirs
        self.allow_relative_paths = allow_relative_paths
        self.allowed_base_dirs: List[Path] = []
        
        if allowed_base_dirs:
            self.allowed_base_dirs = [
                Path(d).resolve() for d in allowed_base_dirs
            ]
    
    def validate_read_path(self, user_path: str) -> Path:
        """
        Validate path for reading operations.
        
        Args:
            user_path: User-provided path string
            
        Returns:
            Resolved Path object
            
        Raises:
            InvalidPathError: If path cannot be resolved
            PathTraversalError: If path traversal is detected
            PathOutsideAllowedDirectoryError: If path is outside allowed dirs
        """
        if not user_path:
            raise InvalidPathError("Path cannot be empty")
        
        resolved = self._resolve_path(user_path)
        self._check_traversal(user_path, resolved)
        
        return resolved
    
    def validate_write_path(
        self,
        user_path: str,
        base_dir: Optional[str] = None
    ) -> Path:
        """
        Validate path for writing operations.
        
        Args:
            user_path: User-provided path string
            base_dir: Optional base directory that path must be within
            
        Returns:
            Resolved Path object
            
        Raises:
            InvalidPathError: If path cannot be resolved
            PathTraversalError: If path traversal is detected
            SystemDirectoryError: If path is in system directory
            PathOutsideAllowedDirectoryError: If path is outside allowed dirs
        """
        if not user_path:
            raise InvalidPathError("Path cannot be empty")
        
        resolved = self._resolve_path(user_path)
        self._check_traversal(user_path, resolved)
        
        # Check if path is within specified base directory
        if base_dir:
            base = Path(base_dir).resolve()
            if not self._is_within_base(resolved, base):
                raise PathOutsideAllowedDirectoryError(
                    f"Path {resolved} must be within {base_dir}"
                )
        
        # Check against system directories
        if self.block_system_dirs and self._is_system_directory(resolved):
            raise SystemDirectoryError(
                f"Cannot write to system directory: {resolved}"
            )
        
        # Ensure parent directory exists or can be created
        parent = resolved.parent
        if not parent.exists():
            try:
                parent.mkdir(parents=True, exist_ok=True)
            except (OSError, PermissionError) as e:
                raise InvalidPathError(
                    f"Cannot create output directory: {e}"
                )
        
        return resolved
    
    def _resolve_path(self, user_path: str) -> Path:
        """
        Resolve path to absolute, handling symlinks.
        
        Args:
            user_path: User-provided path string
            
        Returns:
            Resolved Path object
            
        Raises:
            InvalidPathError: If path cannot be resolved
        """
        try:
            path = Path(user_path)
            # Use resolve(strict=False) to handle non-existent paths
            resolved = path.resolve(strict=False)
            return resolved
        except (OSError, RuntimeError, ValueError) as e:
            raise InvalidPathError(f"Invalid path '{user_path}': {e}")
    
    def _check_traversal(self, original: str, resolved: Path) -> None:
        """
        Check for path traversal attempts.
        
        Args:
            original: Original user-provided path string
            resolved: Resolved Path object
            
        Raises:
            PathTraversalError: If suspicious traversal patterns detected
            PathOutsideAllowedDirectoryError: If path is outside allowed dirs
        """
        # Check for obvious traversal patterns in original path
        suspicious_patterns = ["..", "../", "..\\"]
        has_traversal_pattern = any(
            pattern in original for pattern in suspicious_patterns
        )
        
        if has_traversal_pattern:
            if not self.allow_relative_paths:
                raise PathTraversalError(
                    f"Relative paths with '..' are not allowed: {original}"
                )
            # Log warning even if allowed
            logger.warning(
                f"Relative path with traversal pattern detected: "
                f"{original} -> {resolved}"
            )
        
        # Check against whitelist if provided
        if self.allowed_base_dirs:
            is_allowed = any(
                self._is_within_base(resolved, base)
                for base in self.allowed_base_dirs
            )
            if not is_allowed:
                raise PathOutsideAllowedDirectoryError(
                    f"Path {resolved} is not within allowed directories. "
                    f"Allowed: {[str(d) for d in self.allowed_base_dirs]}"
                )
    
    def _is_within_base(self, path: Path, base: Path) -> bool:
        """
        Check if path is within base directory.
        
        Args:
            path: Path to check
            base: Base directory
            
        Returns:
            True if path is within base, False otherwise
        """
        try:
            # Python 3.9+ has is_relative_to
            return path.is_relative_to(base)
        except AttributeError:
            # Fallback for Python < 3.9
            try:
                path.resolve().relative_to(base.resolve())
                return True
            except ValueError:
                return False
    
    def _is_system_directory(self, path: Path) -> bool:
        """
        Check if path is in system directory.
        
        Args:
            path: Path to check
            
        Returns:
            True if path is in system directory, False otherwise
        """
        system = platform.system().lower()
        system_dirs = self.SYSTEM_DIRECTORIES.get(system, [])
        
        if not system_dirs:
            # Unknown system, be conservative
            logger.warning(f"Unknown system '{system}', using conservative checks")
            return False
        
        # On macOS, allow /private/var (used for temp files) and /tmp
        if system == "darwin":
            resolved_str = str(path.resolve())
            if resolved_str.startswith("/private/var/folders") or resolved_str.startswith("/tmp"):
                return False
        
        # Handle Windows paths specially
        if system == "windows":
            # Get the original path string before resolution
            # On Unix, Path.resolve() may not work correctly for Windows paths
            path_str = str(path)
            resolved_str = str(path.resolve()) if path.exists() else path_str
            
            # Normalize both paths for comparison
            for sys_dir_str in system_dirs:
                sys_path_normalized = sys_dir_str.replace("\\", "/").lower().rstrip("/")
                # Try both original and resolved path strings
                for test_str in [path_str, resolved_str]:
                    test_normalized = test_str.replace("\\", "/").lower()
                    
                    # Remove leading slash if present (Unix representation of Windows path)
                    if test_normalized.startswith("/") and len(test_normalized) > 1:
                        # Check if it's a Windows drive path like /C:/ or C:/
                        if test_normalized[1:2].isalpha() and test_normalized[2:3] == ":":
                            test_normalized = test_normalized[1:]  # Remove leading /
                    
                    # Check if test path starts with system directory
                    if test_normalized.startswith(sys_path_normalized + "/") or test_normalized == sys_path_normalized:
                        return True
            return False
        
        # For non-Windows systems, use standard path resolution
        resolved = path.resolve()
        for sys_dir_str in system_dirs:
            try:
                sys_path = Path(sys_dir_str).resolve()
                if self._is_within_base(resolved, sys_path):
                    return True
            except (OSError, ValueError):
                # Skip invalid system directory paths
                continue
        
        return False
    
    @staticmethod
    def check_user_permissions() -> dict:
        """
        Check if running as privileged user.
        
        Returns:
            Dictionary with permission status and recommendations
        """
        system = platform.system().lower()
        is_admin = False
        username = os.getenv("USER") or os.getenv("USERNAME") or "unknown"
        
        if system == "windows":
            try:
                import ctypes
                is_admin = ctypes.windll.shell32.IsUserAnAdmin() != 0
            except (AttributeError, OSError):
                # Fallback if ctypes not available
                pass
        else:
            # Unix-like systems
            if hasattr(os, "geteuid"):
                is_admin = os.geteuid() == 0
        
        return {
            "is_admin": is_admin,
            "username": username,
            "system": system,
            "recommendation": (
                "WARNING: Running as administrator/root user. "
                "This increases security risk. Consider running as a regular user."
                if is_admin
                else "Running as regular user (recommended)."
            ),
        }

