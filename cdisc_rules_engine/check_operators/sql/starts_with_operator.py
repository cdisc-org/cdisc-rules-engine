from .base_sql_operator import BaseSqlOperator


class StartsWithOperator(BaseSqlOperator):
    """Operator for checking if target starts with comparator."""

    def execute_operator(self, other_value):

        target_column = self.replace_prefix(other_value.get("target"))
        value_is_literal = other_value.get("value_is_literal", False)
        comparator = other_value.get("comparator")

        if isinstance(comparator, list):
            # Convert list to tuple to match dataframe operator behavior
            comparator = tuple(comparator)

        return self._handle_comparator(target_column, comparator, value_is_literal)

    def _handle_comparator(self, target_column, comparator, value_is_literal):
        """Handle any type of comparator (column, literal, tuple, or operation variable)."""
        cache_key = f"{target_column}_starts_with_{str(comparator).replace(' ', '_')}_{value_is_literal}"

        def sql():
            target_sql = self._column_sql(target_column)

            # Determine data source and whether to use EXISTS pattern
            if isinstance(comparator, tuple):
                if not comparator:  # Empty tuple
                    return "FALSE"
                data_source = f"(VALUES {', '.join(f'({self._constant_sql(v)})' for v in comparator)})"
            elif (
                isinstance(comparator, str)
                and comparator in self.operation_variables
                and self.operation_variables[comparator].type == "collection"
            ):
                data_source = self._collection_sql(comparator)
            else:
                # Single value comparison
                comparator_sql = self._sql(comparator, value_is_literal=value_is_literal)
                return f"""NOT ({self._is_empty_sql(target_column)})
                          AND {comparator_sql} != ''
                          AND {target_sql} LIKE {comparator_sql} || '%'"""

            # Multi-value comparison using EXISTS
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND EXISTS (
                          SELECT 1 FROM {data_source} AS values_table(value)
                          WHERE values_table.value != ''
                          AND {target_sql} LIKE values_table.value || '%'
                      )"""

        return self._do_check_operator(cache_key, sql)
