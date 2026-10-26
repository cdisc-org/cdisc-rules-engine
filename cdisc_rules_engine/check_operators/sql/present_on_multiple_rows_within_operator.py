from .base_sql_operator import BaseSqlOperator


class PresentOnMultipleRowsWithinOperator(BaseSqlOperator):
    """Operator for checking if a value appears on multiple rows within groups."""

    def execute_operator(self, other_value):
        """
        Verifies if a value in the 'target' column appears on multiple rows
        within groups defined by the 'within' column.
        """
        target_column = self.replace_prefix(other_value.get("target")).lower()
        min_count = other_value.get("comparator") or 1
        within_column = self.replace_prefix(other_value.get("within")).lower()

        op_name = f"{target_column}_{within_column}_{min_count}_present_on_multiple_rows"

        partition = f"{self._source_column_sql(within_column)}, {self._source_column_sql(target_column)}"
        expression = f"(COUNT(*) OVER (PARTITION BY {partition}) > {min_count})"
        return self._window_column(op_name, expression, column_type="Bool")
