from .base_sql_operator import BaseSqlOperator


class HasDifferentValuesOperator(BaseSqlOperator):
    """Operator for checking if a column has different values."""

    def execute_operator(self, other_value):
        target_column = other_value.get("target").lower()
        operation_name = f"{target_column}_has_different_values"

        return self._do_check_operator(
            operation_name, lambda: f"(SELECT COUNT(DISTINCT {target_column}) FROM {self._table_sql()}) > 1"
        )
