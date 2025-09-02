from .empty_operator import EmptyOperator


class NonEmptyOperator(EmptyOperator):
    """Operator for checking if values are non-empty/not null."""

    def execute_operator(self, other_value):
        # Get result from EmptyOperator and invert it
        empty_result = super().execute_operator(other_value)
        return ~empty_result
