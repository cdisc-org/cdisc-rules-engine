from .base_sql_operator import BaseSqlOperator


class NotOperator(BaseSqlOperator):
    """Generic operator that negates the result of another operator."""

    def __init__(self, data, wrapped_operator):
        super().__init__(data)
        self.wrapped_operator = wrapped_operator(data)

    def execute_operator(self, other_value):
        wrapped_result = self.wrapped_operator.execute_operator(other_value)
        if wrapped_result is not None:
            return ~wrapped_result
        return None
