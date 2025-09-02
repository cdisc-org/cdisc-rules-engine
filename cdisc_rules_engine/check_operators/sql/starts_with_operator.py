from .base_sql_operator import BaseSqlOperator


class StartsWithOperator(BaseSqlOperator):
    """Operator for checking if target starts with comparator."""

    def execute_operator(self, other_value):
        """target = self.replace_prefix(other_value.get("target"))
        comparator = other_value.get("comparator")
        value_is_literal: bool = other_value.get("value_is_literal", False)
        comparison_data = self.get_comparator_data(comparator, value_is_literal)
        if self.validation_df.is_series(comparison_data):
            # need to convert series to tuple to make startswith operator work correctly
            comparison_data: Tuple[str] = tuple(comparison_data)
        results = self.validation_df[target].str.startswith(comparison_data)
        return results"""
        raise NotImplementedError("starts_with check_operator not implemented")
