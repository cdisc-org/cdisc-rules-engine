from abc import abstractmethod

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.exceptions.custom_exceptions import (
    DatasetNotFoundError,
    DomainNotFoundInDefineXMLError,
    EngineError,
    FailedSchemaValidation,
    InvalidDatasetFormat,
    InvalidDictionaryVariable,
    InvalidMatchKeyError,
    MissingDataError,
    NumberOfAttemptsExceeded,
    ReferentialIntegrityError,
    RuleExecutionError,
    RuleFormatError,
    UnsupportedDictionaryType,
    VariableMetadataNotFoundError,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.services import logger


class SqlBaseOperation:
    def __init__(self, params: SqlOperationParams, data_service: PostgresQLDataService):
        self.params = params
        self.data_service = data_service

    @abstractmethod
    def _execute_operation(self):
        raise NotImplementedError(f"Operation {self.__class__.__name__} exists but is not implemented")

    def execute(self) -> SqlOperationResult:
        """
        Execute the operation with error handling.
        Custom exceptions should be allowed to propagate up while other exceptions are logged.
        """
        try:
            logger.info(f"Starting operation {self.__class__.__name__}")
            result = self._execute_operation()
            logger.info(f"Operation {self.__class__.__name__} completed.")
            return result
        except (
            EngineError,
            DatasetNotFoundError,
            ReferentialIntegrityError,
            MissingDataError,
            RuleExecutionError,
            RuleFormatError,
            InvalidMatchKeyError,
            VariableMetadataNotFoundError,
            DomainNotFoundInDefineXMLError,
            InvalidDatasetFormat,
            NumberOfAttemptsExceeded,
            InvalidDictionaryVariable,
            UnsupportedDictionaryType,
            FailedSchemaValidation,
        ) as e:
            logger.debug(f"error in operation {self.__class__.__name__}: {str(e)}")
            raise
        except Exception as e:
            # Log unexpected errors
            logger.error(
                f"error in operation {self.__class__.__name__}: {str(e)}",
                exc_info=True,
            )
            # error_message = str(e)
            # if isinstance(e, TypeError) and any(
            #     phrase in error_message
            #     for phrase in [
            #         "NoneType",
            #         "None",
            #         "object is None",
            #         "'NoneType'",
            #         "None has no attribute",
            #         "unsupported operand type",
            #         "bad operand type",
            #         "object is not",
            #         "cannot be None",
            #     ]
            # ):
            #     return None
            raise

    @staticmethod
    def _replace_variable_wildcards(variables_metadata, domain):
        return [var["name"].replace("--", domain) for var in variables_metadata]
