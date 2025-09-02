from .conformant_value_length_operator import ConformantValueLengthOperator


class NonConformantValueLengthOperator(ConformantValueLengthOperator):
    """Operator for checking if values do NOT conform to expected length."""

    def execute_operator(self, other_value):
        """results = False
        for vlm in self.value_level_metadata:
            results |= self.validation_df.apply(lambda row: vlm["filter"](row) and not vlm["length_check"](row), axis=1)
        return self.validation_df.convert_to_series(results)"""
        raise NotImplementedError("non_conformant_value_length check_operator not implemented")
