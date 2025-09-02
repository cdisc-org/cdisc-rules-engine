from .has_next_corresponding_record_operator import HasNextCorrespondingRecordOperator


class DoesNotHaveNextCorrespondingRecordOperator(HasNextCorrespondingRecordOperator):
    """Operator for checking if record does NOT have next corresponding record."""

    def execute_operator(self, other_value):
        result = super().execute_operator(other_value)
        return ~result
