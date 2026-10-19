from dataclasses import replace

from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.numeric_operation import SqlNumericOperation

VARIABLE_LENGTH_OPERATIONS = {"max_variable_value_length", "global_max_variable_length", "global_min_variable_length"}

VARIABLE_NAME = "variable_name"
VARIABLE_LENGTH = "variable_length"
VARIABLE_MAX_VALUE_LENGTH = "variable_max_value_length"


class SqlVariableLengthOperation(SqlNumericOperation):
    """The largest or smallest length of a variable, grouped by variable name over all datasets."""

    def __init__(
        self,
        params: SqlOperationParams,
        data_service: PostgresQLDataService,
        function: str,
        column: str,
    ):
        super().__init__(
            replace(
                params,
                target=column,
                grouping=[VARIABLE_NAME],
                use_rule_type_table=True,
                filter=None,
                regex=None,
            ),
            data_service,
            function,
        )
