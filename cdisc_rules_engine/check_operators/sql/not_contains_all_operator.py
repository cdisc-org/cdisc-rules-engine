from .contains_all_operator import ContainsAllOperator


class NotContainsAllOperator(ContainsAllOperator):
    """Operator for checking if value does NOT contain all expected elements."""

    def execute_operator(self, other_value):
        result = super().execute_operator(other_value)
        return ~result
