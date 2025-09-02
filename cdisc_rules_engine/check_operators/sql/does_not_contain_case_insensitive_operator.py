from .contains_case_insensitive_operator import ContainsCaseInsensitiveOperator


class DoesNotContainCaseInsensitiveOperator(ContainsCaseInsensitiveOperator):
    """Operator for case-insensitive NOT contains checking."""

    def execute_operator(self, other_value):
        # Get result from ContainsCaseInsensitiveOperator and invert it
        contains_result = super().execute_operator(other_value)
        return ~contains_result
