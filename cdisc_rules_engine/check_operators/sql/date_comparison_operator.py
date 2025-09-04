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
        target_column = self.replace_prefix(other_value.get("target")).lower()
        comparator = other_value.get("comparator")
        value_is_literal = other_value.get("value_is_literal", False)
        date_component = other_value.get("date_component")

        if isinstance(comparator, str) and not value_is_literal:
            comparator = self.replace_prefix(comparator).lower()

        component_suffix = f"_{date_component}" if date_component else ""
        cache_key = f"{target_column}{self.operator}{comparator}{component_suffix}"

        def sql():
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

                if value_is_literal:
                    return f"""CASE WHEN
                        {self.replace_prefix(target_column)} IS NOT NULL
                        AND {self.replace_prefix(target_column)} != ''
                        AND EXTRACT({pg_component} FROM CAST({self.replace_prefix(target_column)} AS TIMESTAMP))
                            {self.operator}
                        EXTRACT({pg_component} FROM CAST('{comparator}' AS TIMESTAMP))
                        THEN true
                        ELSE false
                        END"""
                else:
                    return f"""CASE WHEN
                        {self.replace_prefix(target_column)} IS NOT NULL
                        AND {self.replace_prefix(target_column)} != ''
                        AND {self.replace_prefix(comparator)} IS NOT NULL
                        AND {self.replace_prefix(comparator)} != ''
                        AND EXTRACT({pg_component} FROM CAST({self.replace_prefix(target_column)} AS TIMESTAMP))
                            {self.operator}
                        EXTRACT({pg_component} FROM CAST({self.replace_prefix(comparator)} AS TIMESTAMP))
                        THEN true
                        ELSE false
                        END"""
            else:
                if value_is_literal:
                    return f"""CASE WHEN
                        {target_column} IS NOT NULL
                        AND {target_column} != ''
                        AND CAST({target_column} AS TIMESTAMP)
                            {self.operator}
                        CAST('{comparator}' AS TIMESTAMP)
                        THEN true
                        ELSE false
                        END"""
                else:
                    return f"""CASE WHEN
                        {target_column} IS NOT NULL
                        AND {target_column} != ''
                        AND {comparator} IS NOT NULL
                        AND {comparator} != ''
                        AND CAST({target_column} AS TIMESTAMP)
                            {self.operator}
                        CAST({comparator} AS TIMESTAMP)
                        THEN true
                        ELSE false
                        END"""

        return self._do_check_operator(cache_key, sql)
