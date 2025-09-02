from .present_on_multiple_rows_within_operator import PresentOnMultipleRowsWithinOperator


class NotPresentOnMultipleRowsWithinOperator(PresentOnMultipleRowsWithinOperator):
    """Operator for checking if value is NOT present on multiple rows within group."""

    def execute_operator(self, other_value):
        result = super().execute_operator(other_value)
        return ~result
