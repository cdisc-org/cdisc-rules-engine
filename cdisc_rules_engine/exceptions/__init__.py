"""
CDISC Rules Engine exceptions.
"""

from cdisc_rules_engine.exceptions.custom_exceptions import (
    EngineError,
    DatasetNotFoundError,
    ReferentialIntegrityError,
    MissingDataError,
    RuleExecutionError,
    RuleFormatError,
    InvalidMatchKeyError,
    VariableMetadataNotFoundError,
    DomainNotFoundError,
    DomainNotFoundInDefineXMLError,
    InvalidDatasetFormat,
    InvalidJSONFormat,
    NumberOfAttemptsExceeded,
    InvalidDictionaryVariable,
    UnsupportedDictionaryType,
    FailedSchemaValidation,
    SchemaNotFoundError,
    InvalidSchemaProvidedError,
)

from cdisc_rules_engine.exceptions.path_validation_exceptions import (
    PathValidationError,
    PathTraversalError,
    SystemDirectoryError,
    PathOutsideAllowedDirectoryError,
    InvalidPathError,
)

__all__ = [
    # Base exceptions
    "EngineError",
    # Data exceptions
    "DatasetNotFoundError",
    "InvalidDatasetFormat",
    "InvalidJSONFormat",
    "DomainNotFoundError",
    "DomainNotFoundInDefineXMLError",
    # Rule exceptions
    "RuleExecutionError",
    "RuleFormatError",
    "InvalidMatchKeyError",
    # Metadata exceptions
    "VariableMetadataNotFoundError",
    # Dictionary exceptions
    "InvalidDictionaryVariable",
    "UnsupportedDictionaryType",
    # Schema exceptions
    "FailedSchemaValidation",
    "SchemaNotFoundError",
    "InvalidSchemaProvidedError",
    # General exceptions
    "ReferentialIntegrityError",
    "MissingDataError",
    "NumberOfAttemptsExceeded",
    # Path validation exceptions
    "PathValidationError",
    "PathTraversalError",
    "SystemDirectoryError",
    "PathOutsideAllowedDirectoryError",
    "InvalidPathError",
]

