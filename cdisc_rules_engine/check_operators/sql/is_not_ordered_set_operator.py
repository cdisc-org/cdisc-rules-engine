from .is_ordered_set_operator import IsOrderedSetOperator


class IsNotOrderedSetOperator(IsOrderedSetOperator):
    """Operator for checking if values do NOT form an ordered set."""

    def execute_operator(self, other_value):
        # Get result from IsOrderedSetOperator and invert it
        ordered_result = super().execute_operator(other_value)
        return ~ordered_result
