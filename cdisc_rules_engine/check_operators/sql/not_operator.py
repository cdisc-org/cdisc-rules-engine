from .base_sql_operator import BaseSqlOperator
from .empty_operator import EmptyOperator


class NotOperator(BaseSqlOperator):
    """Generic operator that negates the result of another operator."""

    def __init__(self, data, wrapped_operator):
        super().__init__(data)
        self.wrapped_operator = wrapped_operator(data)

    def execute_operator(self, other_value):
        wrapped_result = self.wrapped_operator.execute_operator(other_value)
        if wrapped_result is not None:
            if self._should_skip_negation(other_value):
                return wrapped_result
            return ~wrapped_result
        return None

    def _should_skip_negation(self, other_value) -> bool:
        if not isinstance(self.wrapped_operator, EmptyOperator):
            return False
        target = self.wrapped_operator.replace_prefix(other_value.get("target"))
        if not isinstance(target, str) or target == "":
            return False
        if target in self.wrapped_operator.operation_variables:
            return False
        return self.wrapped_operator.sql_data_service.pgi.schema.get_column(self.table_id, target) is None
