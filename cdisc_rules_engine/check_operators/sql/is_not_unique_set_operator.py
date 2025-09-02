from .is_unique_set_operator import IsUniqueSetOperator


class IsNotUniqueSetOperator(IsUniqueSetOperator):
    """Operator for checking if values do NOT form a unique set."""

    def execute_operator(self, other_value):
        # Get result from IsUniqueSetOperator and invert it
        unique_result = super().execute_operator(other_value)
        return ~unique_result
