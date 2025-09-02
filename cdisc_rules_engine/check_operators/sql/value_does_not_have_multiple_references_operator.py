from .value_has_multiple_references_operator import ValueHasMultipleReferencesOperator


class ValueDoesNotHaveMultipleReferencesOperator(ValueHasMultipleReferencesOperator):
    """Operator for checking if value does NOT have multiple references."""

    def execute_operator(self, other_value):
        result = super().execute_operator(other_value)
        return ~result
