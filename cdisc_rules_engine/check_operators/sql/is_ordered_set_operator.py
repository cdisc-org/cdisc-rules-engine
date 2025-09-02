from .base_sql_operator import BaseSqlOperator


class IsOrderedSetOperator(BaseSqlOperator):
    """Operator for checking if values form an ordered set."""

    def execute_operator(self, other_value):
        """target = self.replace_prefix(other_value.get("target"))
        value = other_value.get("comparator")
        if not isinstance(value, str):
            raise Exception("Comparator must be a single String value")
        return self.validation_df.is_column_sorted_within(value, target)"""
        raise NotImplementedError("is_ordered_set check_operator not implemented")
