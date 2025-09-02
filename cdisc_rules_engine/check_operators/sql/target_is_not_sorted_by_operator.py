from .target_is_sorted_by_operator import TargetIsSortedByOperator


class TargetIsNotSortedByOperator(TargetIsSortedByOperator):
    """Operator for checking if target is NOT sorted by specified criteria."""

    def execute_operator(self, other_value):
        result = super().execute_operator(other_value)
        return ~result
