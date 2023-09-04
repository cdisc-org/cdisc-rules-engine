from cdisc_rules_engine.enums.base_enum import BaseEnum


class ExecutionStatus(BaseEnum):
    SUCCESS = "success"
    SKIPPED = "skipped"
    EXECUTION_ERROR = "execution_error"
    ISSUE_REPORTED = "issue_reported"
    UNKNOWN_STATUS = "unknown_status"


class ExecutionError(BaseEnum):
    AN_UNKNOWN_EXCEPTION_HAS_OCCURRED = "An unknown exception has occurred"
    COLUMN_NOT_FOUND_IN_DATA = "Column not found in data"
