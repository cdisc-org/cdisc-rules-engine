from .references_correct_codelist_operator import ReferencesCorrectCodelistOperator


class DoesNotReferenceCorrectCodelistOperator(ReferencesCorrectCodelistOperator):
    """Operator for checking if value does NOT reference correct codelist."""

    def execute_operator(self, other_value):
        result = super().execute_operator(other_value)
        return ~result
