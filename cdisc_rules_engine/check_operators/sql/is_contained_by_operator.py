from .base_sql_operator import BaseSqlOperator
from .equal_to_operator import EqualToOperator


class IsContainedByOperator(BaseSqlOperator):
    """Operator for checking if target values are contained by comparator collections."""

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
        target_column = self.replace_prefix(other_value.get("target")).lower()
        value_is_literal = other_value.get("value_is_literal", False)
        comparator = other_value.get("comparator")
        case_insensitive = other_value.get("case_insensitive", False)

        if case_insensitive:
            target_column = f"""LOWER({target_column})"""

        if isinstance(comparator, list):
            # List of literal values - use SQL IN clause
            # TODO: TMP
            if case_insensitive:
                comparator = [str(c).lower() for c in comparator]
            values_list = "', '".join(str(v).replace("'", "''") for v in comparator)
            cache_key = f"{target_column}_contained_by_list"

            def sql():
                return f"""CASE WHEN {target_column} IS NOT NULL
                          AND {target_column} != ''
                          AND {target_column} IN ('{values_list}')
                          THEN true
                          ELSE false
                          END"""

        elif isinstance(comparator, str) and not value_is_literal and self._exists(comparator):
            # Column name provided - check if target value exists anywhere in comparator column
            comparator_column = self.replace_prefix(comparator).lower()
            if case_insensitive:
                comparator_column = f"""LOWER({comparator_column})"""
            cache_key = f"{target_column}_contained_by_{comparator_column}"

            def sql():
                return f"""CASE WHEN {target_column} IS NOT NULL
                          AND {target_column} != ''
                          AND {target_column} IN (
                              SELECT DISTINCT {comparator_column}
                              FROM {self._table_sql()}
                              WHERE {comparator_column} IS NOT NULL
                              AND {comparator_column} != ''
                          )
                          THEN true
                          ELSE false
                          END"""

        else:
            return EqualToOperator(self.original_data, case_insensitive=case_insensitive).execute_operator(
                {
                    "target": other_value.get("target"),
                    "comparator": comparator,
                    "value_is_literal": True,
                }
            )

        return self._do_check_operator(cache_key, sql)
