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


class InvalidOutputVariables(EngineError):
    code = 400
    description = "Invalid output variables"


class VariableMetadataNotFoundError(EngineError):
    code = 400
    description = (
        "Variable metadata is not found in CDISC Library for the provided standard"
    )


class DomainNotFoundInDefineXMLError(Exception):
    code = 400
    description = "Domain is not found in Define XML file"


class InvalidDatasetFormat(Exception):
    code = 400
    description = "Dataset data is malformed."


class NumberOfAttemptsExceeded(Exception):
    pass
