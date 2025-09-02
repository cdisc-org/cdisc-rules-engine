from .equal_to_operator import EqualToOperator


class NotEqualToOperator(EqualToOperator):
    """Operator for inequality comparisons."""

    def execute_operator(self, other_value):
        return super().execute_operator({**other_value, "invert": True})
