from .longer_than_operator import LongerThanOperator


class ShorterThanOrEqualToOperator(LongerThanOperator):
    """Operator for checking if value is shorter than or equal to expected length."""

    def execute_operator(self, other_value):
        result = super().execute_operator(other_value)
        return ~result
