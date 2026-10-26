from .base_sql_operator import BaseSqlOperator


class EmptyWithinExceptLastRowOperator(BaseSqlOperator):
    """Operator for checking if values are empty within group except last row."""

    def execute_operator(self, other_value):
        """
        Returns a boolean for each row indicating if that row's target value is empty
        within its group (last row in each group always returns false).

        Args:
            other_value: Dictionary containing:
                - target: The target column to check for empty values
                - comparator: The column to group by
                - ordering: Optional column to determine the order within groups

        Returns:
            Boolean for each row: true if row is empty and not last in group, false otherwise
        """
        target = self.replace_prefix(other_value.get("target"))
        comparator = self.replace_prefix(other_value.get("comparator"))
        order_by = self.replace_prefix(other_value.get("ordering"))

        if not all([target, comparator]):
            raise ValueError("Missing required parameters: target or comparator")

        cache_key = f"{target}_empty_within_except_last_row_{comparator}"
        if order_by:
            cache_key += f"_ordered_by_{order_by}"

        ordering = self._source_column_sql(order_by) if order_by else "t.id"
        partition = self._source_column_sql(comparator)
        target_sql = self._source_column_sql(target)

        expression = f"""CASE
            WHEN ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {ordering})
                 = COUNT(*) OVER (PARTITION BY {partition}) THEN FALSE
            WHEN {target_sql} IS NULL OR CAST({target_sql} AS TEXT) = '' THEN TRUE
            ELSE FALSE
        END"""
        return self._window_column(cache_key, f"({expression})", column_type="Bool")
