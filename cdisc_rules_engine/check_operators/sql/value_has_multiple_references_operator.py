from .base_sql_operator import BaseSqlOperator


class ValueHasMultipleReferencesOperator(BaseSqlOperator):
    """Operator for checking if value has multiple references."""

    def execute_operator(self, other_value):
        """
        Requires a target column and a reference count column whose values
        are a dictionary containing the number of times that value appears.
        """
        """target: str = self.replace_prefix(other_value.get("target"))
        reference_count_column: str = self.replace_prefix(other_value.get("comparator"))
        result = np.where(
            vectorized_get_dict_key(self.validation_df[reference_count_column], self.validation_df[target]) > 1,
            True,
            False,
        )
        return self.validation_df.convert_to_series(result)"""
        raise NotImplementedError("value_has_multiple_references check_operator not implemented")
