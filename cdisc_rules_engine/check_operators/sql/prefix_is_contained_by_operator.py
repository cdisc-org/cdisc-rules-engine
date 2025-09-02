from .base_sql_operator import BaseSqlOperator
from .is_contained_by_operator import IsContainedByOperator


class PrefixIsContainedByOperator(BaseSqlOperator):
    """Operator for checking if target prefix is contained by the comparator."""

    def execute_operator(self, other_value):
        """
        Checks if target prefix is contained by the comparator.
        """
        target = self.replace_prefix(other_value.get("target"))
        prefix_length = other_value.get("prefix")
        prefix_sql = f"LEFT({target}, {prefix_length})"

        other_value["target"] = prefix_sql

        return IsContainedByOperator(self.original_data).execute_operator(other_value)
