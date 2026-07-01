from .base_sql_operator import BaseSqlOperator
from .equal_to_operator import EqualToOperator


class IsContainedByOperator(BaseSqlOperator):
    """Operator for checking if target values are contained by comparator collections."""

    def __init__(self, data, case_insensitive=False):
        super().__init__(data)
        self.case_insensitive = case_insensitive

    def execute_operator(self, other_value):
        """
        Checks if the target column values are contained within the comparator.

        Returns True if target value exists in the comparator collection/column and is not null/empty.
        Returns False if target value is null, empty, or not found in comparator.

        Handles three types of comparators:
        1. List of literal values - check if target is in the list
        2. Column name (when value_is_literal=False) - checks if target value exists anywhere in the comparator column
        3. Single literal value - checks direct equality with the target
        """
        target = other_value.get("target")
        value_is_literal = other_value.get("value_is_literal", False)
        comparator = other_value.get("comparator")

        prefix = other_value.get("prefix", None)
        suffix = other_value.get("suffix", None)

        if isinstance(target, str) and target in self.operation_variables:
            target_var = self.operation_variables[target]

            if target_var.type == "collection":
                if isinstance(comparator, str) and not value_is_literal and comparator in self.operation_variables:
                    return self._handle_collection_vs_collection(target, comparator)
                elif isinstance(comparator, list):
                    return self._handle_collection_vs_list(target, comparator)

        target_column = self.replace_prefix(target)
        column = self._column_sql(target_column, lowercase=self.case_insensitive, prefix=prefix, suffix=suffix)

        if isinstance(comparator, list) or (isinstance(comparator, str) and comparator in self.operation_variables):
            comparator_sql = self._collection_sql(comparator, lowercase=self.case_insensitive)

            def sql():
                return f"""NOT ({self._is_empty_sql(target_column)})
                          AND {column} IN {comparator_sql}"""

        elif isinstance(comparator, str) and not value_is_literal and self._exists(comparator):
            comparator_sql = self._column_sql(comparator, lowercase=self.case_insensitive, alias=False)

            def sql():
                return f"""NOT ({self._is_empty_sql(target_column)})
                          AND {column} IN (
                              SELECT DISTINCT {comparator_sql}
                              FROM {self._table_sql()}
                              WHERE NOT ({self._is_empty_sql(comparator, alias=False)})
                          )"""

        else:
            return EqualToOperator(self.original_data, case_insensitive=self.case_insensitive).execute_operator(
                {
                    "target": target,
                    "comparator": comparator,
                    "value_is_literal": True,
                }
            )

        return self._do_check_operator(sql)

    def _handle_collection_vs_collection(self, target, comparator):
        def sql():
            target_sql = self._collection_sql(target, lowercase=self.case_insensitive)
            comparator_sql = self._collection_sql(comparator, lowercase=self.case_insensitive)

            return f"""NOT EXISTS (
                SELECT 1 FROM {target_sql} AS target_vals(t_val)
                WHERE target_vals.t_val IS NOT NULL AND target_vals.t_val != ''
                AND NOT EXISTS (
                    SELECT 1 FROM {comparator_sql} AS comp_vals(c_val)
                    WHERE comp_vals.c_val IS NOT NULL AND comp_vals.c_val != ''
                    AND comp_vals.c_val = target_vals.t_val
                )
            )"""

        return self._do_check_operator(sql)

    def _handle_collection_vs_list(self, target_collection, comparator_list):
        def sql():
            target_sql = self._collection_sql(target_collection, lowercase=self.case_insensitive)
            values_sql = ", ".join(
                f"({self._constant_sql(v, lowercase=self.case_insensitive)})" for v in comparator_list
            )

            return f"""NOT EXISTS (
                SELECT 1 FROM {target_sql} AS target_vals(t_val)
                WHERE target_vals.t_val IS NOT NULL AND target_vals.t_val != ''
                AND NOT EXISTS (
                    SELECT 1 FROM (VALUES {values_sql}) AS comp_vals(c_val)
                    WHERE comp_vals.c_val = target_vals.t_val
                )
            )"""

        return self._do_check_operator(sql)
