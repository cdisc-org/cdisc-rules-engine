"""
Custom exceptions for path validation errors.
"""

from cdisc_rules_engine.exceptions.custom_exceptions import EngineError


class PathValidationError(EngineError):
    """Base exception for all path validation errors."""
    
    code = 400
    description = "Path validation error"


class PathTraversalError(PathValidationError):
    """Raised when path traversal is detected."""
    
    code = 400
    description = "Path traversal detected - path contains '..' or attempts to access parent directories"


class SystemDirectoryError(PathValidationError):
    """Raised when attempting to access system directories."""
    
    code = 403
    description = "Access to system directory is not allowed"


class PathOutsideAllowedDirectoryError(PathValidationError):
    """Raised when path is outside allowed directory boundaries."""
    
    code = 403
    description = "Path is outside allowed directory boundaries"


class InvalidPathError(PathValidationError):
    """Raised when path is invalid or cannot be resolved."""
    
    code = 400
    description = "Invalid path provided"

