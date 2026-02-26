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

        def generate_update_query(db_table: str, db_column: str) -> str:
            # Construct the UPDATE query. This uses a window function to count
            # occurrences within each group and then updates each row accordingly.
            return f"""
                UPDATE {db_table} AS t
                SET {db_column} = sub.is_present
                FROM (
                    SELECT
                        id,
                        (COUNT(*) OVER (PARTITION BY {within_column}, {target_column})) > {min_count} AS is_present
                    FROM {db_table}
                    ORDER BY id
                ) AS sub
                WHERE t.id = sub.id;
            """

        result_series = self._do_complex_check_operator(op_name, generate_update_query)

        return result_series
