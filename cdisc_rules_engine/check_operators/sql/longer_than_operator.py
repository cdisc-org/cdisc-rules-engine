from .base_sql_operator import BaseSqlOperator


class LongerThanOperator(BaseSqlOperator):
    """Operator for checking if value is longer than expected length."""

    def execute_operator(self, other_value):
        """
        Checks if the target is longer than the comparator.
        If comparing two columns (value_is_literal is False), the operator
        compares lengths of values in these columns.
        """
        """target = self.replace_prefix(other_value.get("target"))
        comparator = other_value.get("comparator")
        value_is_literal: bool = other_value.get("value_is_literal", False)
        comparison_data = self.get_comparator_data(comparator, value_is_literal)
        if self.validation_df.is_series(comparison_data):
            if is_integer_dtype(comparison_data):
                results = self.validation_df[target].str.len().gt(comparison_data)
            else:
                results = self.validation_df[target].str.len().gt(comparison_data.str.len())
        else:
            results = self.validation_df[target].str.len().gt(comparison_data)
        return results"""
        raise NotImplementedError("longer_than check_operator not implemented")
