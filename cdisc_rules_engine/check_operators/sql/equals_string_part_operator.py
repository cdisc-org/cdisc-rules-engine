from .base_sql_operator import BaseSqlOperator


class EqualsStringPartOperator(BaseSqlOperator):
    """Operator for checking if string part equals comparator."""

    def __init__(self, data, invert=False):
        super().__init__(data)
        self.invert = invert

    def execute_operator(self, other_value):
        """
        Checks that the values in the target column
        equal the result of parsing the value in the comparison
        column with a regex
        """
        target = self.replace_prefix(other_value.get("target"))
        target_column = self._column_sql(target)
        comparator = other_value.get("comparator")
        regex = other_value.get("regex")
        value_is_literal = other_value.get("value_is_literal", False)

        if not value_is_literal:
            comparator = self.replace_prefix(comparator)

        comparator_column = (
            self._column_sql(comparator) if not value_is_literal else self._sql(comparator, value_is_literal=True)
        )

        if self.invert:
            operator_name = f"{target}_does_not_equal_string_part_{comparator}_{regex}"
        else:
            operator_name = f"{target}_equals_string_part_{comparator}_{regex}"

        def sql():
            extracted_part = f"(regexp_match({comparator_column}::text, '{regex}'))[1]"
            equality_check = f"{target_column}::text = {extracted_part}"

            if self.invert:
                return f"""CASE
                        WHEN {self._is_empty_sql(target)} THEN FALSE
                        WHEN {comparator_column} IS NULL OR {comparator_column}::text = '' THEN FALSE
                        WHEN {extracted_part} IS NULL THEN FALSE
                        WHEN {equality_check} THEN FALSE
                        ELSE TRUE
                        END"""
            else:
                return f"""CASE
                        WHEN {self._is_empty_sql(target)} THEN FALSE
                        WHEN {comparator_column} IS NULL OR {comparator_column}::text = '' THEN FALSE
                        WHEN {extracted_part} IS NULL THEN FALSE
                        ELSE {equality_check}
                        END"""

        return self._do_check_operator(operator_name, sql)
