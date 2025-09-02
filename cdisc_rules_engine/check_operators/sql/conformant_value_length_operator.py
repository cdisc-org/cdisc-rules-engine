from .base_sql_operator import BaseSqlOperator


class ConformantValueLengthOperator(BaseSqlOperator):
    """Operator for checking if values conform to expected length."""

    def execute_operator(self, other_value):
        """results = False
        for vlm in self.value_level_metadata:
            results |= self.validation_df.apply(lambda row: vlm["filter"](row) and vlm["length_check"](row), axis=1)
        return self.validation_df.convert_to_series(results)"""
        raise NotImplementedError("conformant_value_length check_operator not implemented")
