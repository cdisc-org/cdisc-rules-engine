from .contains_operator import ContainsOperator


class DoesNotContainOperator(ContainsOperator):
    """Operator for checking if target does NOT contain comparator values."""

    def execute_operator(self, other_value):
        # Get result from ContainsOperator and invert it
        contains_result = super().execute_operator(other_value)
        return ~contains_result
