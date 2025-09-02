from .is_ordered_by_operator import IsOrderedByOperator


class IsNotOrderedByOperator(IsOrderedByOperator):
    """Operator for checking if data is NOT ordered by specified columns."""

    def execute_operator(self, other_value):
        result = super().execute_operator(other_value)
        return ~result
