from .base_sql_operator import BaseSqlOperator


class EmptyOperator(BaseSqlOperator):
    """Operator for checking if values are empty/null."""

    def _execute_operator_impl(self, other_value):
        column = self.replace_prefix(other_value.get("target"))

        def sql():
            if self.sql_data_service.pgi.schema.get_column(self.table_id, column) is None:
                return "TRUE"

            return self._is_empty_sql(column)

        return self._do_check_operator(f"{column}_empty", sql)

    def get_result_for_missing_columns(self):
        return "TRUE"  # matches the behaviour above but method is required for consistency
