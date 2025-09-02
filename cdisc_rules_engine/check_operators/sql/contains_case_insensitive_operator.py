from .base_sql_operator import BaseSqlOperator


class ContainsCaseInsensitiveOperator(BaseSqlOperator):
    """Operator for case-insensitive contains checking."""

    def execute_operator(self, other_value):
        """target = self.replace_prefix(other_value.get("target"))
        value_is_literal = other_value.get("value_is_literal", False)
        comparator = (
            self.replace_prefix(other_value.get("comparator"))
            if not value_is_literal
            else other_value.get("comparator")
        )
        comparison_data = self.get_comparator_data(comparator, value_is_literal)
        comparison_data = self.convert_string_data_to_lower(comparison_data)
        if self.is_column_of_iterables(self.validation_df[target]):
            results = vectorized_case_insensitive_is_in(comparison_data, self.validation_df[target])
        elif self.validation_df.is_series(comparison_data):
            results = self._series_is_in(
                self.convert_string_data_to_lower(self.validation_df[target]),
                self.convert_string_data_to_lower(comparison_data),
            )
        else:
            results = vectorized_case_insensitive_is_in(comparison_data.lower(), self.validation_df[target])
        return self.validation_df.convert_to_series(results)"""
        raise NotImplementedError("contains_case_insensitive check_operator not implemented")
