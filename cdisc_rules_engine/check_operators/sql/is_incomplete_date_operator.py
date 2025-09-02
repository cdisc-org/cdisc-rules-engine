from .is_complete_date_operator import IsCompleteDateOperator


class IsIncompleteDateOperator(IsCompleteDateOperator):
    """Operator for checking if date is incomplete."""

    def execute_operator(self, other_value):
        # Get result from IsCompleteDateOperator and invert it
        complete_result = super().execute_operator(other_value)
        return ~complete_result
