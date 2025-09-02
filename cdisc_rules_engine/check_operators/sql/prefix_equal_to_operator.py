from .base_sql_operator import BaseSqlOperator


class PrefixEqualToOperator(BaseSqlOperator):
    """Operator for checking if prefix equals to expected value."""

    def execute_operator(self, other_value):
        """
        Checks if target prefix is equal to comparator.
        """
        """target: str = self.replace_prefix(other_value.get("target"))
        value_is_literal: bool = other_value.get("value_is_literal", False)
        comparator: Union[str, Any] = (
            self.replace_prefix(other_value.get("comparator"))
            if not value_is_literal
            else other_value.get("comparator")
        )
        if comparator == "DOMAIN":
            comparison_data = self.column_prefix_map["--"]
        else:
            comparison_data = self.get_comparator_data(comparator, value_is_literal)
        prefix: int = self.replace_prefix(other_value.get("prefix"))
        return self._check_equality_of_string_part(target, comparison_data, "prefix", prefix)"""
        raise NotImplementedError("prefix_equal_to check_operator not implemented")
