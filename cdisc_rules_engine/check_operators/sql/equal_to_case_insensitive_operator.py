from .equal_to_operator import EqualToOperator


class EqualToCaseInsensitiveOperator(EqualToOperator):
    """Operator for case-insensitive equality comparisons."""

    def execute_operator(self, other_value):
        return super().execute_operator({**other_value, "case_insensitive": True})
