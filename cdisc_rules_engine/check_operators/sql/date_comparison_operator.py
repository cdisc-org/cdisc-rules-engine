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

        if isinstance(target, str) and self._exists(target.lower()):
            target = target.lower()
            target_date_column = self.sql_data_service.pgi.generate_date_column(self.table_id, target)
            wrapped_target = target_date_column.hash
        else:
            wrapped_target = f"CAST ({self._sql(target)} AS TIMESTAMP)"

        if isinstance(comparator, str) and not value_is_literal:
            comparator = self.replace_prefix(comparator)

        if isinstance(comparator, str) and not value_is_literal and self._exists(comparator.lower()):
            comparator = comparator.lower()
            comparator_date_column = self.sql_data_service.pgi.generate_date_column(self.table_id, comparator)
            wrapped_comparator = comparator_date_column.hash
        else:
            wrapped_comparator = f"CAST ({self._sql(comparator, value_is_literal=value_is_literal)} AS TIMESTAMP)"

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

        return self._do_check_operator(sql)
