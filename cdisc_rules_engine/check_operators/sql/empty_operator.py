from .base_sql_operator import BaseSqlOperator


class EmptyOperator(BaseSqlOperator):
    """Operator for checking if values are empty/null."""

    def execute_operator(self, other_value):
        column = self.replace_prefix(other_value.get("target"))

        def sql():
            if self.sql_data_service.pgi.schema.get_column(self.table_id, column) is None:
                return "TRUE"

            return self._is_empty_sql(column)

        return self._do_check_operator(f"{column}_empty", sql)
