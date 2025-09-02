from .is_not_unique_relationship_operator import IsNotUniqueRelationshipOperator


class IsUniqueRelationshipOperator(IsNotUniqueRelationshipOperator):
    """Operator for checking unique relationships between columns."""

    def execute_operator(self, other_value):
        # Get result from IsNotUniqueRelationshipOperator and invert it
        not_unique_result = super().execute_operator(other_value)
        return ~not_unique_result
