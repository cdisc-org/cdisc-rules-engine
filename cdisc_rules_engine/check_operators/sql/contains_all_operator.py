from .base_sql_operator import BaseSqlOperator


class ContainsAllOperator(BaseSqlOperator):
    """Operator for checking if value contains all expected elements."""

    def execute_operator(self, other_value):
        """
        Checks if the target column contains all values from the comparator.

        This operator returns a single boolean value (as a series) for the entire dataset,
        not per-row results like other operators.

        """
        target = other_value.get("target")
        comparator = other_value.get("comparator")

        if target in self.operation_variables:
            target_var = self.operation_variables[target]
            if target_var.type == "collection":
                return self._handle_target_is_collection(target, comparator)

        target_column = self.replace_prefix(target).lower()

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

            def sql():
                return "true"

        else:

            def sql():
                values_clause = ", ".join(f"({self._constant_sql(v)})" for v in comparator)
                return f"""CASE WHEN (
                              SELECT COUNT(DISTINCT val)
                              FROM (VALUES {values_clause}) AS comparator_values(val)
                              WHERE val IN (
                                  SELECT DISTINCT {self._column_sql(target_column, alias=False)}
                                  FROM {self._table_sql()}
                                  WHERE NOT ({self._is_empty_sql(target_column, alias=False)})
                              )
                          ) = {len(comparator)}
                          THEN true
                          ELSE false
                          END"""

        return self._do_check_operator(sql)

    def _handle_operation_variable_comparator(self, target_column, comparator):
        """Handle when comparator is an operation variable."""
        variable = self.operation_variables[comparator]

        if variable.type == "collection":
            return self._handle_collection_variable(target_column, comparator)
        elif variable.type == "constant":
            return self._handle_constant_variable(target_column, comparator)
        else:
            raise ValueError(
                f"Unsupported operation variable type '{variable.type}' for contains_all operation "
                f"on column '{target_column}'. Expected 'collection' or 'constant'."
            )

    def _handle_collection_variable(self, target_column, comparator):
        """Handle collection type operation variable."""
        collection_sql = self._collection_sql(comparator)

        def sql():
            return f"""CASE WHEN (
                          SELECT COUNT(DISTINCT value)
                          FROM {collection_sql} AS op_var
                          WHERE value IS NOT NULL
                          AND value != ''
                          AND value IN (
                              SELECT DISTINCT {self._column_sql(target_column, alias=False)}
                              FROM {self._table_sql()}
                              WHERE NOT ({self._is_empty_sql(target_column, alias=False)})
                          )
                      ) = (
                          SELECT COUNT(DISTINCT value)
                          FROM {collection_sql} AS op_var
                          WHERE value IS NOT NULL
                          AND value != ''
                      )
                      THEN true
                      ELSE false
                      END"""

        return self._do_check_operator(sql)

    def _handle_constant_variable(self, target_column, comparator):
        """Handle constant type operation variable."""
        constant_sql = self._constant_sql(comparator)

        def sql():
            return f"""CASE WHEN {constant_sql} IN (
                          SELECT DISTINCT {self._column_sql(target_column, alias=False)}
                          FROM {self._table_sql()}
                          WHERE NOT ({self._is_empty_sql(target_column, alias=False)})
                      )
                      THEN true
                      ELSE false
                      END"""

        return self._do_check_operator(sql)

    def _handle_column_comparator(self, target_column, comparator):
        """Handle when comparator is a column name."""
        # Column name provided - check if all values from comparator column exist in target column
        comparator_column = self.replace_prefix(comparator).lower()

        def sql():
            return f"""CASE WHEN (
                          SELECT COUNT(DISTINCT {self._column_sql(comparator_column, alias=False)})
                          FROM {self._table_sql()}
                          WHERE NOT ({self._is_empty_sql(comparator_column, alias=False)})
                          AND {self._column_sql(comparator_column, alias=False)} IN (
                              SELECT DISTINCT {self._column_sql(target_column, alias=False)}
                              FROM {self._table_sql()}
                              WHERE NOT ({self._is_empty_sql(target_column, alias=False)})
                          )
                      ) = (
                          SELECT COUNT(DISTINCT {self._column_sql(comparator_column, alias=False)})
                          FROM {self._table_sql()}
                          WHERE NOT ({self._is_empty_sql(comparator_column, alias=False)})
                      )
                      THEN true
                      ELSE false
                      END"""

        return self._do_check_operator(sql)

    def _handle_target_is_collection(self, target_variable, comparator):
        """Handle when the target is a collection operation variable."""

        def sql():
            collection_sql = self._collection_sql(target_variable)
            comparator_sql = self._constant_sql(comparator)

            return f"""CASE
                        WHEN NOT EXISTS (SELECT 1 FROM {collection_sql}) THEN false
                        WHEN EXISTS (
                            SELECT 1 FROM {collection_sql} AS collection_values(value)
                            WHERE collection_values.value IS NULL
                               OR collection_values.value = ''
                               OR collection_values.value NOT LIKE '%' || {comparator_sql} || '%'
                        ) THEN false
                        ELSE true
                       END"""

        return self._do_check_operator(sql)

    def _handle_invalid_comparator(self, target_column):
        """Handle invalid comparator types."""
        raise ValueError(
            f"Invalid comparator type for contains_all operation on column '{target_column}'. "
            "Expected list, column name, or operation variable."
        )
