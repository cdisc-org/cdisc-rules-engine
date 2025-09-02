from .is_ordered_subset_of_operator import IsOrderedSubsetOfOperator


class IsNotOrderedSubsetOfOperator(IsOrderedSubsetOfOperator):
    """Operator for checking if value is NOT an ordered subset of another value."""

    def execute_operator(self, other_value):
        result = super().execute_operator(other_value)
        return ~result
