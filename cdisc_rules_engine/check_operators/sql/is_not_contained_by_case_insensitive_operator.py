from .is_contained_by_case_insensitive_operator import IsContainedByCaseInsensitiveOperator


class IsNotContainedByCaseInsensitiveOperator(IsContainedByCaseInsensitiveOperator):
    """Operator for case-insensitive NOT containment checks."""

    def execute_operator(self, other_value):
        # Get result from IsContainedByCaseInsensitiveOperator and invert it
        contained_result = super().execute_operator(other_value)
        return ~contained_result
