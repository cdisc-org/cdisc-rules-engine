from .longer_than_or_equal_to_operator import LongerThanOrEqualToOperator


class ShorterThanOperator(LongerThanOrEqualToOperator):
    """Operator for checking if value is shorter than expected length."""

    def execute_operator(self, other_value):
        result = super().execute_operator(other_value)
        return ~result
