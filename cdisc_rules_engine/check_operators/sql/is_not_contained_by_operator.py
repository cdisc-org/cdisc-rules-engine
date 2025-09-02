from .is_contained_by_operator import IsContainedByOperator


class IsNotContainedByOperator(IsContainedByOperator):
    """Operator for checking if target values are NOT contained by comparator collections."""

    def execute_operator(self, other_value):
        # Get result from IsContainedByOperator and invert it
        contained_result = super().execute_operator(other_value)
        return ~contained_result
