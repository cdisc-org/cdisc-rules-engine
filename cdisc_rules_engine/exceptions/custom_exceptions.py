class EngineError(Exception):
    """All custom API Exceptions"""

    def __init__(self, message=None):
        self.message = message


class DatasetNotFoundError(EngineError):
    code = 404
    description = "Dataset not found"


class ReferentialIntegrityError(EngineError):
    description = "This action violates referential integrity"


class MissingDataError(EngineError):
    description = "Necessary data missing"


class RuleExecutionError(EngineError):
    code = 500
    description = "Issue executing rule"


class RuleFormatError(EngineError):
    code = 400
    description = "Improperly formatted rule"


class InvalidMatchKeyError(EngineError):
    code = 400
    description = "Invalid match key provided"


class VariableMetadataNotFoundError(EngineError):
    code = 400
    description = (
        "Variable metadata is not found in CDISC Library for the provided standard"
    )


LIBRARY_METADATA_NOT_FOUND_HINT = (
    "Check your standard/version (CLI) or Library tab values (editor)."
)


def library_metadata_not_found_message(standard, version, substandard=None):
    version_display = (version or "").replace("-", ".")
    sub_part = f" substandard {substandard}" if substandard else ""
    return (
        f"No library metadata found for standard '{standard}' "
        f"version '{version_display}'{sub_part}. {LIBRARY_METADATA_NOT_FOUND_HINT}"
    )


class LibraryMetadataNotFoundError(EngineError):
    code = 400
    description = (
        "Library metadata not found for the provided standard and version combination. "
        f"{LIBRARY_METADATA_NOT_FOUND_HINT}"
    )


class DomainNotFoundError(EngineError):
    """Raised when a required domain is not found in the dataset"""

    code = 404
    description = "Domain Not Found"


class DomainNotFoundInDefineXMLError(EngineError):
    code = 400
    description = "Domain is not found in Define XML file"


class InvalidDatasetFormat(EngineError):
    code = 400
    description = "Dataset data is malformed."


INVALID_DATASET_FORMAT_REASON = (
    "may be corrupted, incorrectly formatted, or encoded with an unexpected encoding."
)


class InvalidJSONFormat(EngineError):
    code = 400
    description = "JSON data is malformed."


class ExcelTestDataError(EngineError):
    code = 400
    description = (
        "Excel test data file is missing required sheets or column headers. "
        "Sheet and column names are case-sensitive."
    )


class NumberOfAttemptsExceeded(EngineError):
    pass


class InvalidDictionaryVariable(EngineError):
    description = (
        "Provided dictionary variable does not correspond to a dictionary term type"
    )


class UnsupportedDictionaryType(EngineError):
    description = "Specified dictionary type is not supported by the rules engine."


class FailedSchemaValidation(EngineError):
    description = "Error Occured in Schema Validation"


class SchemaNotFoundError(EngineError):
    code = 404
    description = "XSD file could not be found"


class InvalidSchemaProvidedError(EngineError):
    code = 400
    description = "Failed to parse XMLSchema"


class PreprocessingError(EngineError):
    description = "Error occurred during dataset preprocessing"


class OperationError(EngineError):
    description = "Error occurred during operation execution"


class DatasetBuilderError(EngineError):
    description = "Error occurred during dataset building"


class DateTimeParserError(EngineError):
    code = 400
    description = "Failure to parse a datetime string"
