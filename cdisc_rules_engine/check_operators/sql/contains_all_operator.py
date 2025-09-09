from .base_sql_operator import BaseSqlOperator


class ContainsAllOperator(BaseSqlOperator):
    """Operator for checking if value contains all expected elements."""

    def execute_operator(self, other_value):
        """
        Checks if the target column contains all values from the comparator.

        This operator returns a single boolean value (as a series) for the entire dataset,
        not per-row results like other operators.

        """
        target_column = self.replace_prefix(other_value.get("target")).lower()
        comparator = other_value.get("comparator")

        if isinstance(comparator, list):
            return self._handle_list_comparator(target_column, comparator)
        elif isinstance(comparator, str) and comparator in self.operation_variables:
            return self._handle_operation_variable_comparator(target_column, comparator)
        elif isinstance(comparator, str) and self._exists(comparator):
            return self._handle_column_comparator(target_column, comparator)
        else:
            return self._handle_invalid_comparator(target_column)

    def _handle_list_comparator(self, target_column, comparator):
        """Handle when comparator is a list."""
        if len(comparator) == 0:
            cache_key = f"{target_column}_contains_all_empty_list"

            def sql():
                return "true"

        else:
            cache_key = f"{target_column}_contains_all_list"

            def sql():
                values_clause = ", ".join(f"({self._constant_sql(v)})" for v in comparator)
                return f"""CASE WHEN (
                              SELECT COUNT(DISTINCT val)
                              FROM (VALUES {values_clause}) AS comparator_values(val)
                              WHERE val IN (
                                  SELECT DISTINCT {self._column_sql(target_column)}
                                  FROM {self._table_sql()}
                                  WHERE NOT ({self._is_empty_sql(target_column)})
                              )
                          ) = {len(comparator)}
                          THEN true
                          ELSE false
                          END"""

        return self._do_check_operator(cache_key, sql)

    def _handle_operation_variable_comparator(self, target_column, comparator):
        """Handle when comparator is an operation variable."""
        variable = self.operation_variables[comparator]
        cache_key = f"{target_column}_contains_all_opvar_{comparator}"

        if variable.type == "collection":
            return self._handle_collection_variable(target_column, comparator, cache_key)
        elif variable.type == "constant":
            return self._handle_constant_variable(target_column, comparator, cache_key)
        else:
            raise ValueError(
                f"Unsupported operation variable type '{variable.type}' for contains_all operation "
                f"on column '{target_column}'. Expected 'collection' or 'constant'."
            )

    def _handle_collection_variable(self, target_column, comparator, cache_key):
        """Handle collection type operation variable."""
        collection_sql = self._collection_sql(comparator)

        def sql():
            return f"""CASE WHEN (
                          SELECT COUNT(DISTINCT column1)
                          FROM {collection_sql} AS op_var
                          WHERE column1 IS NOT NULL
                          AND column1 != ''
                          AND column1 IN (
                              SELECT DISTINCT {self._column_sql(target_column)}
                              FROM {self._table_sql()}
                              WHERE NOT ({self._is_empty_sql(target_column)})
                          )
                      ) = (
                          SELECT COUNT(DISTINCT column1)
                          FROM {collection_sql} AS op_var
                          WHERE column1 IS NOT NULL
                          AND column1 != ''
                      )
                      THEN true
                      ELSE false
                      END"""

        return self._do_check_operator(cache_key, sql)

    def _handle_constant_variable(self, target_column, comparator, cache_key):
        """Handle constant type operation variable."""
        constant_sql = self._constant_sql(comparator)

        def sql():
            return f"""CASE WHEN {constant_sql} IN (
                          SELECT DISTINCT {self._column_sql(target_column)}
                          FROM {self._table_sql()}
                          WHERE NOT ({self._is_empty_sql(target_column)})
                      )
                      THEN true
                      ELSE false
                      END"""

        return self._do_check_operator(cache_key, sql)

    def _handle_column_comparator(self, target_column, comparator):
        """Handle when comparator is a column name."""
        # Column name provided - check if all values from comparator column exist in target column
        comparator_column = self.replace_prefix(comparator).lower()
        cache_key = f"{target_column}_contains_all_{comparator_column}"

        def sql():
            return f"""CASE WHEN (
                          SELECT COUNT(DISTINCT {self._column_sql(comparator_column)})
                          FROM {self._table_sql()}
                          WHERE NOT ({self._is_empty_sql(comparator_column)})
                          AND {self._column_sql(comparator_column)} IN (
                              SELECT DISTINCT {self._column_sql(target_column)}
                              FROM {self._table_sql()}
                              WHERE NOT ({self._is_empty_sql(target_column)})
                          )
                      ) = (
                          SELECT COUNT(DISTINCT {self._column_sql(comparator_column)})
                          FROM {self._table_sql()}
                          WHERE NOT ({self._is_empty_sql(comparator_column)})
                      )
                      THEN true
                      ELSE false
                      END"""

        return self._do_check_operator(cache_key, sql)

    def _handle_invalid_comparator(self, target_column):
        """Handle invalid comparator types."""
        raise ValueError(
            f"Invalid comparator type for contains_all operation on column '{target_column}'. "
            "Expected list, column name, or operation variable."
        )
