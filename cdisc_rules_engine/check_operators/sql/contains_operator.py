from .base_sql_operator import BaseSqlOperator


class ContainsOperator(BaseSqlOperator):
    """Operator for checking if target contains comparator values."""

    def execute_operator(self, other_value):
        """target = self.replace_prefix(other_value.get("target"))
        value_is_literal = other_value.get("value_is_literal", False)
        comparator = (
            self.replace_prefix(other_value.get("comparator"))
            if not value_is_literal
            else other_value.get("comparator")
        )
        comparison_data = self.get_comparator_data(comparator, value_is_literal)
        if self.is_column_of_iterables(self.validation_df[target]) or isinstance(comparison_data, str):
            results = vectorized_is_in(comparison_data, self.validation_df[target])
        elif self.validation_df.is_series(comparison_data):
            results = self._series_is_in(self.validation_df[target], comparison_data)
        else:
            # Handles numeric case. This case should never occur
            results = np.where(self.validation_df[target] == comparison_data, True, False)
        return self.validation_df.convert_to_series(results)"""
        raise NotImplementedError("contains check_operator not implemented")
