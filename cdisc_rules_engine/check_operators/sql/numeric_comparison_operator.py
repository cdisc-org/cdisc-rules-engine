from .base_sql_operator import BaseSqlOperator


class NumericComparisonOperator(BaseSqlOperator):
    """Operator for numeric comparisons."""

    def __init__(self, data, operator="<"):
        super().__init__(data)
        self.operator = operator

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target"))
        comparator = self.replace_prefix(other_value.get("comparator"))
        target_column = self._sql(target)
        comparator_column = self._sql(comparator)

        def sql():
            return f"""NOT ({self._is_empty_sql(target)})
                        AND NOT ({self._is_empty_sql(comparator)})
                        AND CAST({target_column} AS NUMERIC)
                                {self.operator}
                            CAST({comparator_column} AS NUMERIC)
                        """

        return self._do_check_operator(f"{target_column}{self.operator}{comparator}", sql)
