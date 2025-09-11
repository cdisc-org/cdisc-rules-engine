from .base_sql_operator import BaseSqlOperator


class StringLengthComparisonOperator(BaseSqlOperator):
    """Operator for string length comparisons."""

    def __init__(self, data, operator=">"):
        super().__init__(data)
        self.operator = operator

    def execute_operator(self, other_value):
        target_column = self.replace_prefix(other_value.get("target")).lower()
        comparator = other_value.get("comparator")
        value_is_literal = other_value.get("value_is_literal", False)

        def sql():
            target_length = f"LENGTH(CAST({self._sql(target_column)} AS TEXT))"

            if self._is_numeric_value(comparator, value_is_literal):
                comparator_expr = self._sql(comparator, value_is_literal=value_is_literal)
            else:
                comparator_expr = f"LENGTH(CAST({self._sql(comparator, value_is_literal=value_is_literal)} AS TEXT))"

            return f"""CASE WHEN {self._is_empty_sql(target_column)} THEN FALSE
                           WHEN {target_length} {self.operator} {comparator_expr} THEN TRUE
                           ELSE FALSE END"""

        return self._do_check_operator(
            f"{target_column}_length_{self.operator}_{str(comparator).replace(' ', '_')}", sql
        )
