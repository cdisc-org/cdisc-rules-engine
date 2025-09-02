from .base_sql_operator import BaseSqlOperator


class InvalidDateOperator(BaseSqlOperator):
    """Operator for checking if date is invalid."""

    def execute_operator(self, other_value):
        """target = self.replace_prefix(other_value.get("target"))
        results = ~vectorized_is_valid(self.validation_df[target])
        return self.validation_df.convert_to_series(results)"""
        raise NotImplementedError("invalid_date check_operator not implemented")
