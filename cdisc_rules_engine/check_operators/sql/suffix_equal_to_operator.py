from .base_sql_operator import BaseSqlOperator


class SuffixEqualToOperator(BaseSqlOperator):
    """Operator for checking if suffix equals to expected value."""

    def execute_operator(self, other_value):
        """
        Checks if target suffix is equal to comparator.
        """
        """target: str = self.replace_prefix(other_value.get("target"))
        value_is_literal: bool = other_value.get("value_is_literal", False)
        comparator: Union[str, Any] = (
            self.replace_prefix(other_value.get("comparator"))
            if not value_is_literal
            else other_value.get("comparator")
        )
        comparison_data = self.get_comparator_data(comparator, value_is_literal)
        suffix: int = self.replace_prefix(other_value.get("suffix"))
        return self._check_equality_of_string_part(target, comparison_data, "suffix", suffix)"""
        raise NotImplementedError("suffix_equal_to check_operator not implemented")
