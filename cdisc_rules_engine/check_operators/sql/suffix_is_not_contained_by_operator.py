from .suffix_is_contained_by_operator import SuffixIsContainedByOperator


class SuffixIsNotContainedByOperator(SuffixIsContainedByOperator):
    """Operator for checking if target suffix is NOT contained by the comparator."""

    def execute_operator(self, other_value):
        # Get result from SuffixIsContainedByOperator and invert it
        contained_result = super().execute_operator(other_value)
        return ~contained_result
