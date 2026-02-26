from .base_sql_operator import BaseSqlOperator


class VariableMetadataEqualToOperator(BaseSqlOperator):
    """Operator for checking if variable metadata equals to expected value."""

    def execute_operator(self, other_value):
        """
        Validates the metadata for variables,
        provided in the metadata column, is equal to
        the comparator.
        Ex.
        target: STUDYID
        comparator: "Exp"
        metadata_column: {"STUDYID": "Req", "DOMAIN": "Req"}
        result: False
        """
        """target = self.replace_prefix(other_value.get("target"))
        comparator = other_value.get("comparator")  # Assumes the comparator is a value not a column
        metadata_column = self.replace_prefix(other_value.get("metadata"))
        result = np.where(
            vectorized_get_dict_key(self.validation_df[metadata_column], target) == comparator,
            True,
            False,
        )
        return self.validation_df.convert_to_series(result)"""
        raise NotImplementedError("variable_metadata_equal_to check_operator not implemented")
