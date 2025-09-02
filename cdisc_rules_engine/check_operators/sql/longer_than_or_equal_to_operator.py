from .base_sql_operator import BaseSqlOperator


class LongerThanOrEqualToOperator(BaseSqlOperator):
    """Operator for checking if value is longer than or equal to expected length."""

    def execute_operator(self, other_value):
        """target = self.replace_prefix(other_value.get("target"))
        comparator = other_value.get("comparator")
        value_is_literal: bool = other_value.get("value_is_literal", False)
        comparison_data = self.get_comparator_data(comparator, value_is_literal)
        if self.validation_df.is_series(comparison_data):
            if is_integer_dtype(comparison_data):
                results = self.validation_df[target].str.len().ge(comparison_data)
            else:
                results = self.validation_df[target].str.len().ge(comparison_data.str.len())
        else:
            results = self.validation_df[target].str.len().ge(comparator)
        return results"""
        raise NotImplementedError("longer_than_or_equal_to check_operator not implemented")
