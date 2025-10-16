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
from cdisc_rules_engine.models.library_metadata_container import LibraryMetadataContainer
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.utilities import sdtm_utilities
from cdisc_rules_engine.utilities.utils import convert_library_class_name_to_ct_class
from cdisc_rules_engine.utilities.sdtm_utilities import get_class_and_domain_metadata
from typing import List, Optional


class SqlOperationError(Exception):
    """Simple exception to identify which SQL operation caused the error."""

    def __init__(self, original_exception, operation_name):
        self.original_exception = original_exception
        self.operation_name = operation_name
        super().__init__(f"{operation_name}: {str(original_exception)}")


class SqlBaseOperation:
    def __init__(
        self,
        params: SqlOperationParams,
        data_service: PostgresQLDataService,
        library_metadata=LibraryMetadataContainer(),
    ):
        """
        Initialize the SQL base operation.
        """
        self.params = params
        self.data_service = data_service
        self.library_metadata = library_metadata

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
            # Log unexpected errors and wrap with operation context
            logger.error(
                f"error in operation {self.__class__.__name__}: {str(e)}",
                exc_info=True,
            )

            raise SqlOperationError(original_exception=e, operation_name=self.__class__.__name__.lower()) from e

    def construct_where_clause(self) -> str:
        """
        Construct a WHERE clause from the provided filter conditions.
        """
        if not self.params.filter:
            return ""

        where_clauses = []
        for column, value in self.params.filter.items():
            column_sql = self.data_service.pgi.schema.get_column_hash(self.params.domain, column)
            if isinstance(value, str):
                where_clauses.append(f"{column_sql} = '{value.replace('\'', '\'\'')}'")
            elif isinstance(value, (int, float)):
                where_clauses.append(f"{column_sql} = {value}")
            else:
                raise ValueError(f"Unsupported filter value type: {type(value)} for column {column}")

        return "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    def get_dataset_class(self, domain: str) -> Optional[str]:
        """
        Get the dataset class for a given domain, similar to BaseDataService.get_dataset_class
        but adapted for SQL operations without requiring a dataset parameter.
        """
        if self.library_metadata.standard_metadata:
            class_data, _ = get_class_and_domain_metadata(
                self.library_metadata.standard_metadata,
                domain,
            )
            name = class_data.get("name")
            if name:
                return convert_library_class_name_to_ct_class(name)

        # For SQL operations, we need to handle special cases without access to the actual dataset
        # We can use the data service to check for specific columns if needed
        return self._handle_special_cases_sql(domain)

    def _handle_special_cases_sql(self, domain: str) -> Optional[str]:
        """
        Handle special cases for SQL operations when determining dataset class.
        This is adapted from BaseDataService._handle_special_cases but works with SQL schema.
        """
        from cdisc_rules_engine.constants.classes import (
            FINDINGS,
            FINDINGS_ABOUT,
            EVENTS,
            INTERVENTIONS,
            RELATIONSHIP,
        )

        # Check if columns exist in the schema for this domain
        try:
            if self._column_exists_in_domain(domain, "TERM"):
                return EVENTS
            if self._column_exists_in_domain(domain, "TRT"):
                return INTERVENTIONS
            if self._column_exists_in_domain(domain, "QNAM"):
                return RELATIONSHIP
            if self._column_exists_in_domain(domain, "TESTCD"):
                if self._column_exists_in_domain(domain, "OBJ"):
                    return FINDINGS_ABOUT
                return FINDINGS
            # Note: Associated Persons (AP--) handling would require more complex logic
            # that may not be suitable for SQL operations without dataset content analysis
        except Exception:
            # If we can't determine the class through schema inspection, return None
            pass

        return None

    def _column_exists_in_domain(self, domain: str, column_suffix: str) -> bool:
        """
        Check if a column exists in the given domain's schema.
        For topic variables, constructs the full column name as DOMAIN + column_suffix.
        """
        try:
            # For topic variables, check if DOMAIN + suffix exists (e.g., AETERM, CMTRT, etc.)
            topic_column = domain.upper() + column_suffix
            if self.data_service.pgi.schema.column_exists(domain, topic_column):
                return True

            # Also check for the suffix alone (e.g., TERM, TRT, etc.) in case of RDOMAIN scenarios
            if self.data_service.pgi.schema.column_exists(domain, column_suffix):
                return True

            return False
        except Exception:
            return False

    def _get_variables_metadata_from_standard_model(self, domain: str) -> List[dict]:
        """
        Gets variables metadata for the given class and domain from cache.
        The cache stores CDISC Library metadata.
        SQL implementation that doesn't require a dataframe parameter.

        Return example:
        [
            {
               "label":"Study Identifier",
               "name":"STUDYID",
               "ordinal":"1",
               "role":"Identifier",
               ...
            },
            {
               "label":"Domain Abbreviation",
               "name":"DOMAIN",
               "ordinal":"2",
               "role":"Identifier"
            },
            ...
        ]
        """
        # TODO: Update to handle multiple standard types.

        # For SQL operations, use a simplified version that works with available metadata
        model_details = self.library_metadata.model_metadata

        # Handle SUPP domain normalization like the original function
        if domain and (domain.upper().startswith("SUPP") or domain.upper().startswith("SQ")) and len(domain) > 2:
            domain = "SUPPQUAL"

        domain_details = sdtm_utilities.get_model_domain_metadata(model_details, domain)
        variables_metadata = []
        class_name = None

        if domain_details:
            # Domain found in the model
            class_name = convert_library_class_name_to_ct_class(domain_details["_links"]["parentClass"]["title"])
            class_details = sdtm_utilities.get_class_metadata(model_details, class_name)
            variables_metadata = domain_details.get("datasetVariables", [])
            if variables_metadata:
                variables_metadata.sort(key=lambda item: int(item["ordinal"]))
        else:
            # Domain not found in the model. Use the new get_dataset_class method
            class_name = self.get_dataset_class(domain)

            if class_name is None:
                # Fall back to General Observations class for unknown domains
                from cdisc_rules_engine.constants.classes import GENERAL_OBSERVATIONS_CLASS

                class_name = GENERAL_OBSERVATIONS_CLASS

            class_details = sdtm_utilities.get_class_metadata(model_details, class_name)

        # Apply class-specific logic for detectable classes
        from cdisc_rules_engine.constants.classes import DETECTABLE_CLASSES

        if class_name and class_name in DETECTABLE_CLASSES:
            (
                identifiers_metadata,
                class_variables_metadata,
                timing_metadata,
            ) = sdtm_utilities.get_allowed_class_variables(model_details, class_details)
            # Identifiers are added to the beginning and Timing to the end
            variables_metadata = class_variables_metadata
            if identifiers_metadata:
                variables_metadata = identifiers_metadata + variables_metadata
            if timing_metadata:
                variables_metadata = variables_metadata + timing_metadata

        return variables_metadata

    @staticmethod
    def _replace_variable_wildcards(variables_metadata, domain):
        return [var["name"].replace("--", domain) for var in variables_metadata]
