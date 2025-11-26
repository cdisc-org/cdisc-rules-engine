import re
from .base_sql_operator import BaseSqlOperator


class InconsistentEnumeratedColumnsOperator(BaseSqlOperator):
    """Operator for checking inconsistent enumerated columns."""

    def _execute_operator_impl(self, other_value):
        """
        Check for inconsistencies in enumerated columns of a DataFrame.

        Starting with the smallest/largest enumeration of the given variable,
        return an error if VARIABLE(N+1) is populated but VARIABLE(N) is not populated.
        Repeat for all variables belonging to the enumeration.
        Note that the initial variable will not have an index (VARIABLE) and
        the next enumerated variable has index 1 (VARIABLE1).
        """
        target_variable = self.replace_prefix(other_value.get("target"))

        matching_columns = self._find_enumerated_columns(target_variable)

        if not matching_columns:
            cache_key = f"{target_variable}_inconsistent_enumerated_no_matches"
            return self._do_check_operator(cache_key, lambda: "FALSE")

        cache_key = f"{target_variable}_inconsistent_enumerated_columns"

        def generate_update_query(db_table: str, db_column: str) -> str:
            return self._generate_inconsistency_check_query(db_table, db_column, matching_columns)

        return self._do_complex_check_operator(cache_key, generate_update_query)

    def _find_enumerated_columns(self, target_variable: str) -> list:
        """
        Find all columns that match the enumeration pattern for the target variable.
        Returns them sorted in enumeration order (base variable first, then numbered).
        """
        table_schema = self.sql_data_service.pgi.schema.get_table(self.table_id)
        if not table_schema:
            return []

        all_columns = table_schema.get_columns()
        matching_columns = []

        # Pattern to match VARIABLE, VARIABLE1, VARIABLE2, etc.
        pattern = rf"^{re.escape(target_variable)}(\d*)$"

        for column_name, column_schema in all_columns:
            if re.match(pattern, column_name, re.IGNORECASE):
                matching_columns.append(column_name.lower())

        def sort_key(col_name):
            match = re.match(rf"^{re.escape(target_variable)}(\d*)$", col_name, re.IGNORECASE)
            if match:
                suffix = match.group(1)
                if suffix == "":
                    return (0, col_name)
                else:
                    return (int(suffix), col_name)
            return (float("inf"), col_name)

        return sorted(matching_columns, key=sort_key)

    def _generate_inconsistency_check_query(self, db_table: str, db_column: str, columns: list) -> str:
        """
        Generate SQL query to check for enumeration inconsistencies.

        The original dataframe logic:
        1. Start with first column's populated state
        2. For each subsequent column:
           - If current column is populated AND previous column was NOT populated -> inconsistency
           - Update previous state to current column's state

        We can implement this by checking each column against its immediate predecessor.
        """
        if len(columns) <= 1:
            # No inconsistencies possible with only one column
            return f"""
                UPDATE {db_table} AS t
                SET {db_column} = FALSE;
            """

        inconsistency_conditions = []

        for i in range(1, len(columns)):
            current_col = columns[i]
            prev_col = columns[i - 1]

            current_col_schema = self.sql_data_service.pgi.schema.get_column(self.table_id, current_col)
            prev_col_schema = self.sql_data_service.pgi.schema.get_column(self.table_id, prev_col)

            current_populated = self._build_populated_condition(current_col, current_col_schema)
            prev_populated = self._build_populated_condition(prev_col, prev_col_schema)

            condition = f"({current_populated} AND NOT ({prev_populated}))"
            inconsistency_conditions.append(condition)

        main_condition = " OR ".join(inconsistency_conditions) if inconsistency_conditions else "FALSE"

        return f"""
            UPDATE {db_table} AS t
            SET {db_column} = sub.is_inconsistent
            FROM (
                SELECT
                    id,
                    CASE
                        WHEN {main_condition} THEN TRUE
                        ELSE FALSE
                    END AS is_inconsistent
                FROM {db_table} AS t1
                ORDER BY id
            ) AS sub
            WHERE t.id = sub.id;
        """

    def _build_populated_condition(self, column: str, column_schema) -> str:
        """Build SQL condition to check if a column is populated."""
        col_sql = self._column_sql(column, alias=False)

        if column_schema and column_schema.type == "Num":
            return f"({col_sql} IS NOT NULL)"
        else:
            return f"({col_sql} IS NOT NULL AND {col_sql} != '')"

    def get_result_for_missing_columns(self):
        return "FALSE"
