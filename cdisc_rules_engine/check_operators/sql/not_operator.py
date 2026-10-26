from .base_sql_operator import BaseSqlOperator


class NotOperator(BaseSqlOperator):
    """Generic operator that negates the result of another operator."""

    def __init__(self, data, wrapped_operator):
        super().__init__(data)
        self.wrapped_operator = wrapped_operator(data)

    def execute_operator(self, other_value):
        wrapped_result = self.wrapped_operator.execute_operator(other_value)
        if wrapped_result is None:
            return None
        if self.wrapped_operator.has_missing_required_columns(other_value):
            return wrapped_result
        return f"NOT COALESCE(({wrapped_result}), FALSE)"
