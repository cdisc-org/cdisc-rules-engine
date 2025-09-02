from .has_different_values_operator import HasDifferentValuesOperator


class HasSameValuesOperator(HasDifferentValuesOperator):
    """Operator for checking if a column has same values."""

    def execute_operator(self, other_value):
        # Get result from HasDifferentValuesOperator and invert it
        different_result = super().execute_operator(other_value)
        return ~different_result
