from .has_equal_length_operator import HasEqualLengthOperator


class HasNotEqualLengthOperator(HasEqualLengthOperator):
    """Operator for checking if values do NOT have equal length."""

    def execute_operator(self, other_value):
        result = super().execute_operator(other_value)
        return ~result
