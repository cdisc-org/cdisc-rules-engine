from .variable_metadata_equal_to_operator import VariableMetadataEqualToOperator


class VariableMetadataNotEqualToOperator(VariableMetadataEqualToOperator):
    """Operator for checking if variable metadata does NOT equal to expected value."""

    def execute_operator(self, other_value):
        result = super().execute_operator(other_value)
        return ~result
