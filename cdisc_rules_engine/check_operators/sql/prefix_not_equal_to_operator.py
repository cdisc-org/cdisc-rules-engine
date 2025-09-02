from .prefix_equal_to_operator import PrefixEqualToOperator


class PrefixNotEqualToOperator(PrefixEqualToOperator):
    """Operator for checking if prefix does NOT equal to expected value."""

    def execute_operator(self, other_value):
        """
        Checks if target prefix is not equal to comparator.
        """
        result = super().execute_operator(other_value)
        return ~result
