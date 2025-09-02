from .suffix_equal_to_operator import SuffixEqualToOperator


class SuffixNotEqualToOperator(SuffixEqualToOperator):
    """Operator for checking if suffix does NOT equal to expected value."""

    def execute_operator(self, other_value):
        result = super().execute_operator(other_value)
        return ~result
