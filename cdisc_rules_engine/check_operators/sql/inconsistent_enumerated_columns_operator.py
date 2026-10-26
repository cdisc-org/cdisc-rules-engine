import re
from .base_sql_operator import BaseSqlOperator


class InconsistentEnumeratedColumnsOperator(BaseSqlOperator):
    """Operator for checking inconsistent enumerated columns."""

    def execute_operator(self, other_value):
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
            return self._do_check_operator(lambda: "FALSE")

        return self._do_check_operator(lambda: f"({self._inconsistency_condition(matching_columns)})")

    def _inconsistency_condition(self, columns: list, alias: bool = True) -> str:
        """
        An enumeration is inconsistent when VARIABLE(N+1) is populated while
        VARIABLE(N) is not, for any adjacent pair.
        """
        if len(columns) <= 1:
            return "FALSE"

        conditions = []
        for previous, current in zip(columns, columns[1:]):
            current_populated = self._build_populated_condition(
                current, self.sql_data_service.pgi.schema.get_column(self.table_id, current), alias=alias
            )
            previous_populated = self._build_populated_condition(
                previous, self.sql_data_service.pgi.schema.get_column(self.table_id, previous), alias=alias
            )
            conditions.append(f"({current_populated} AND NOT ({previous_populated}))")
        return " OR ".join(conditions)

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

    def _build_populated_condition(self, column: str, column_schema, alias: bool = False) -> str:
        """Build SQL condition to check if a column is populated."""
        col_sql = self._column_sql(column, alias=alias)

        if column_schema and column_schema.type == "Num":
            return f"({col_sql} IS NOT NULL)"
        else:
            return f"({col_sql} IS NOT NULL AND {col_sql} != '')"
