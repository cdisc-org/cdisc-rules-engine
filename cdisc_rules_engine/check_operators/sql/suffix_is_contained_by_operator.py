from .base_sql_operator import BaseSqlOperator
from .is_contained_by_operator import IsContainedByOperator


class SuffixIsContainedByOperator(BaseSqlOperator):
    """Operator for checking if target suffix is contained by the comparator."""

    def execute_operator(self, other_value):
        """
        Checks if target suffix is contained by the comparator.
        """
        target = self.replace_prefix(other_value.get("target"))
        suffix_length = other_value.get("suffix")
        suffix_sql = f"RIGHT({target}, {suffix_length})"

        other_value["target"] = suffix_sql
        return IsContainedByOperator(self.original_data).execute_operator(other_value)
