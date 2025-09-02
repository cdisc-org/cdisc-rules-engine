from .equals_string_part_operator import EqualsStringPartOperator


class DoesNotEqualStringPartOperator(EqualsStringPartOperator):
    """Operator for checking if string part does NOT equal comparator."""

    def execute_operator(self, other_value):
        # Get result from EqualsStringPartOperator and invert it
        equals_result = super().execute_operator(other_value)
        return ~equals_result
