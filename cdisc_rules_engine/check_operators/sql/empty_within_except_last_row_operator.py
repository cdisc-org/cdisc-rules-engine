from .base_sql_operator import BaseSqlOperator


class EmptyWithinExceptLastRowOperator(BaseSqlOperator):
    """Operator for checking if values are empty within group except last row."""

    def _execute_operator_impl(self, other_value):
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

        def sql(table_name, column_name):
            return f"""
            WITH ranked AS (
                SELECT
                    id,
                    {self._column_sql(target, alias=False)} AS target_val,
                    ROW_NUMBER() OVER (
                        PARTITION BY {self._column_sql(comparator, alias=False)}
                        ORDER BY {self._column_sql(order_by, alias=False) if order_by else 'id'}
                    ) AS rn,
                    COUNT(*) OVER (
                        PARTITION BY {self._column_sql(comparator, alias=False)}
                    ) AS cnt
                FROM {table_name}
            )
            UPDATE {table_name} t
            SET {column_name} = (
                SELECT
                    CASE
                        -- Last row in group is always false
                        WHEN r.rn = r.cnt THEN false
                        -- Note: We don't use _is_empty_sql here because we're working with a column alias (target_val)
                        WHEN r.target_val IS NULL OR r.target_val = '' THEN true
                        ELSE false
                    END
                FROM ranked r
                WHERE r.id = t.id
            )
            """

        return self._do_complex_check_operator(cache_key, sql)

    def get_result_for_missing_columns(self):
        return "TRUE"
