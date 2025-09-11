from .base_sql_operator import BaseSqlOperator


class DateComparisonOperator(BaseSqlOperator):
    """Operator for date comparisons."""

    def __init__(self, data, operator="="):
        super().__init__(data)
        self.operator = operator

    def execute_operator(self, other_value):
        """
        Performs date comparison operations in PostgreSQL.
        Handles date component extraction and comparison.
        """
        target = self.replace_prefix(other_value.get("target"))
        comparator = other_value.get("comparator")
        value_is_literal = other_value.get("value_is_literal", False)
        date_component = other_value.get("date_component")

        if isinstance(comparator, str) and not value_is_literal:
            comparator = self.replace_prefix(comparator).lower()

        target_date_column = self.sql_data_service.pgi.generate_date_column(self.table_id, target)

        if self.sql_data_service.pgi.schema.get_column(self.table_id, comparator) is None:
            comparator_sql = f"CAST ({self._sql(comparator, value_is_literal=value_is_literal)} AS TIMESTAMP)"
        else:
            comparator_date_column = self.sql_data_service.pgi.generate_date_column(self.table_id, comparator)
            comparator_sql = comparator_date_column.hash

        component_suffix = f"_{date_component}" if date_component else ""
        cache_key = f"{target}{self.operator}{comparator}{component_suffix}"

        wrapped_target = target_date_column.hash
        wrapped_comparator = comparator_sql

        if date_component:
            component_map = {
                "year": "YEAR",
                "month": "MONTH",
                "day": "DAY",
                "hour": "HOUR",
                "minute": "MINUTE",
                "second": "SECOND",
                "microsecond": "MICROSECONDS",
            }
            pg_component = component_map.get(date_component, "EPOCH")
            wrapped_target = f"EXTRACT({pg_component} FROM {wrapped_target})"
            wrapped_comparator = f"EXTRACT({pg_component} FROM {wrapped_comparator})"

        def sql():
            return f"""CASE WHEN
                NOT ({self._is_empty_sql(target)})
                AND NOT ({self._is_empty_sql(comparator)})
                AND {wrapped_target} {self.operator} {wrapped_comparator}
                THEN true
                ELSE false
                END"""

        return self._do_check_operator(cache_key, sql)
