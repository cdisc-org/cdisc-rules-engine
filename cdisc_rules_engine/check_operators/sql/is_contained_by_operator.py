from .base_sql_operator import BaseSqlOperator


class IsContainedByOperator(BaseSqlOperator):
    """Operator for checking if target values are contained by comparator collections."""

    def __init__(self, data, case_insensitive=False):
        super().__init__(data)
        self.case_insensitive = case_insensitive

    def execute_operator(self, other_value):
        """
        Checks if the target column/array values are contained within the comparator.

        Returns True if target value exists in the comparator collection/column and is not null/empty.
        Returns False if target value is null, empty, or not found in comparator.

        Handles these types of comparators:
        1. List of literal values - check if target is in the list
        2. Operation variable (collection) - check if target is in the operation result set
        3. Operation variable (constant) - checks direct equality with the target
        4. Column name (when value_is_literal=False) - checks if target value exists anywhere in the comparator column
        5. Single literal value - checks direct equality with the target
        """
        target = other_value.get("target")
        value_is_literal = other_value.get("value_is_literal", False)
        comparator = other_value.get("comparator")

        prefix = other_value.get("prefix", None)
        suffix = other_value.get("suffix", None)

        is_target_list = isinstance(target, list)
        is_target_collection = (
            isinstance(target, str)
            and target in self.operation_variables
            and self.operation_variables[target].type == "collection"
        )

        if is_target_list or is_target_collection:
            return self._handle_array_target(target, comparator, value_is_literal, is_target_list)

        target_sql, target_empty_sql = self._get_scalar_target_sql(target, prefix, suffix)
        return self._handle_scalar_target(target_sql, target_empty_sql, comparator, value_is_literal)

    def _get_scalar_target_sql(self, target, prefix, suffix):
        target_name = self.replace_prefix(target)

        if self._exists(target_name):
            target_sql = self._column_sql(target_name, lowercase=self.case_insensitive, prefix=prefix, suffix=suffix)
            target_empty_sql = self._is_empty_sql(target_name)
        else:
            base_target_sql = self._constant_sql(target_name, lowercase=self.case_insensitive)
            if prefix is not None:
                base_target_sql = f"LEFT({base_target_sql}, {prefix})"
            if suffix is not None:
                base_target_sql = f"RIGHT({base_target_sql}, {suffix})"

            target_sql = base_target_sql
            target_empty_sql = "FALSE" if target_name else "TRUE"

        return target_sql, target_empty_sql

    def _handle_scalar_target(self, target_sql, target_empty_sql, comparator, value_is_literal):
        if isinstance(comparator, list):
            if not comparator:
                return self._do_check_operator(lambda: "FALSE")
            comparator_sql = self._collection_sql(comparator, lowercase=self.case_insensitive)
            return self._do_check_operator(lambda: f"NOT ({target_empty_sql}) AND {target_sql} IN {comparator_sql}")

        elif isinstance(comparator, str) and comparator in self.operation_variables:
            var = self.operation_variables[comparator]
            if var.type == "constant":
                comp_sql = self._constant_sql(comparator, lowercase=self.case_insensitive)
                return self._do_check_operator(lambda: f"NOT ({target_empty_sql}) AND {target_sql} = {comp_sql}")
            else:
                comp_sql = self._collection_sql(comparator, lowercase=self.case_insensitive)
                return self._do_check_operator(lambda: f"NOT ({target_empty_sql}) AND {target_sql} IN {comp_sql}")

        elif isinstance(comparator, str) and not value_is_literal and self._exists(self.replace_prefix(comparator)):
            comp_col = self.replace_prefix(comparator)
            comp_sql = self._column_sql(comp_col, lowercase=self.case_insensitive, alias=False)
            comp_empty = self._is_empty_sql(comp_col, alias=False)

            def sql():
                return f"""NOT ({target_empty_sql})
                          AND {target_sql} IN (
                              SELECT DISTINCT {comp_sql}
                              FROM {self._table_sql()}
                              WHERE NOT ({comp_empty})
                          )"""

            return self._do_check_operator(sql)

        else:
            comp_sql = self._constant_sql(comparator, lowercase=self.case_insensitive)
            return self._do_check_operator(lambda: f"NOT ({target_empty_sql}) AND {target_sql} = {comp_sql}")

    def _handle_array_target(self, target, comparator, value_is_literal, is_target_list):
        if is_target_list:
            if not target:
                return self._do_check_operator(lambda: "FALSE")
            values = ", ".join(f"({self._constant_sql(v, lowercase=self.case_insensitive)})" for v in target)
            target_table_sql = f"(VALUES {values})"
        else:
            target_table_sql = self._collection_sql(target, lowercase=self.case_insensitive)

        if isinstance(comparator, list):
            if not comparator:
                return self._do_check_operator(lambda: "FALSE")
            comp_values = ", ".join(f"({self._constant_sql(v, lowercase=self.case_insensitive)})" for v in comparator)
            comp_table_sql = f"(VALUES {comp_values})"

        elif isinstance(comparator, str) and comparator in self.operation_variables:
            var = self.operation_variables[comparator]
            if var.type == "collection":
                comp_table_sql = self._collection_sql(comparator, lowercase=self.case_insensitive)
            else:
                comp_sql = self._constant_sql(comparator, lowercase=self.case_insensitive)
                comp_table_sql = f"(VALUES ({comp_sql}))"

        elif isinstance(comparator, str) and not value_is_literal and self._exists(self.replace_prefix(comparator)):
            comp_col = self.replace_prefix(comparator)
            comp_sql = self._column_sql(comp_col, lowercase=self.case_insensitive, alias=False)
            comp_empty = self._is_empty_sql(comp_col, alias=False)
            comp_table_sql = f"(SELECT DISTINCT {comp_sql} FROM {self._table_sql()} WHERE NOT ({comp_empty}))"

        else:
            comp_sql = self._constant_sql(comparator, lowercase=self.case_insensitive)
            comp_table_sql = f"(VALUES ({comp_sql}))"

        def sql():
            return f"""NOT EXISTS (
                SELECT 1 FROM {target_table_sql} AS target_vals(t_val)
                WHERE target_vals.t_val IS NOT NULL AND target_vals.t_val != ''
                AND NOT EXISTS (
                    SELECT 1 FROM {comp_table_sql} AS comp_vals(c_val)
                    WHERE comp_vals.c_val IS NOT NULL AND comp_vals.c_val != ''
                    AND comp_vals.c_val = target_vals.t_val
                )
            )"""

        return self._do_check_operator(sql)
