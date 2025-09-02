from .base_sql_operator import BaseSqlOperator


class HasEqualLengthOperator(BaseSqlOperator):
    """Operator for checking if values have equal length."""

    def execute_operator(self, other_value):
        """
        Checks that the target length is the same as comparator.
        If comparing two columns (value_is_literal is False), the operator
        compares lengths of values in these columns.
        """
        """target = self.replace_prefix(other_value.get("target"))
        comparator = other_value.get("comparator")
        value_is_literal: bool = other_value.get("value_is_literal", False)
        comparison_data = self.get_comparator_data(comparator, value_is_literal)
        if self.validation_df.is_series(comparison_data):
            if is_integer_dtype(comparison_data):
                results = self.validation_df[target].str.len().eq(comparison_data).astype(bool)
            else:
                results = self.validation_df[target].str.len().eq(comparison_data.str.len()).astype(bool)
        else:
            results = self.validation_df[target].str.len().eq(comparator).astype(bool)
        return results"""
        raise NotImplementedError("has_equal_length check_operator not implemented")
