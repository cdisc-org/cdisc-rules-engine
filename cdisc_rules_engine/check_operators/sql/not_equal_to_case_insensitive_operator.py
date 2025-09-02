from .equal_to_operator import EqualToOperator


class NotEqualToCaseInsensitiveOperator(EqualToOperator):
    """Operator for case-insensitive inequality comparisons."""

    def execute_operator(self, other_value):
        return super().execute_operator({**other_value, "case_insensitive": True, "invert": True})
