from .prefix_is_contained_by_operator import PrefixIsContainedByOperator


class PrefixIsNotContainedByOperator(PrefixIsContainedByOperator):
    """Operator for checking if target prefix is NOT contained by the comparator."""

    def execute_operator(self, other_value):
        contained_result = super().execute_operator(other_value)
        return ~contained_result
