from .is_contained_by_operator import IsContainedByOperator


class IsContainedByCaseInsensitiveOperator(IsContainedByOperator):
    """Operator for case-insensitive containment checks."""

    def execute_operator(self, other_value):
        return super().execute_operator({**other_value, "case_insensitive": True})
