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
        target_column = self.replace_prefix(other_value.get("target"))
        value_is_literal = other_value.get("value_is_literal", False)
        comparator = other_value.get("comparator")

        prefix = other_value.get("prefix", None)
        suffix = other_value.get("suffix", None)

        column = self._column_sql(target_column, lowercase=self.case_insensitive, prefix=prefix, suffix=suffix)

        cache_key = (
            f"{target_column}_contained_by_{comparator}_{value_is_literal}_{self.case_insensitive}_{prefix}_{suffix}"
        )

        if isinstance(comparator, list) or (isinstance(comparator, str) and comparator in self.operation_variables):

            def sql():
                return f"""NOT ({self._is_empty_sql(target_column)})
                          AND {column} IN {self._collection_sql(comparator, lowercase=self.case_insensitive)}"""

        elif isinstance(comparator, str) and not value_is_literal and self._exists(comparator):

            def sql():
                return f"""NOT ({self._is_empty_sql(target_column)})
                          AND {column} IN (
                              SELECT DISTINCT {self._column_sql(comparator, lowercase=self.case_insensitive)}
                              FROM {self._table_sql()}
                              WHERE NOT ({self._is_empty_sql(comparator)})
                          )"""

        else:
            return EqualToOperator(self.original_data, case_insensitive=self.case_insensitive).execute_operator(
                {
                    "target": other_value.get("target"),
                    "comparator": comparator,
                    "value_is_literal": True,
                }
            )

        return self._do_check_operator(cache_key, sql)
