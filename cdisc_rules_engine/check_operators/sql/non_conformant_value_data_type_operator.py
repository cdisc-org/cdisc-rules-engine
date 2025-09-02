from .conformant_value_data_type_operator import ConformantValueDataTypeOperator


class NonConformantValueDataTypeOperator(ConformantValueDataTypeOperator):
    """Operator for checking if values do NOT conform to expected data type."""

    def execute_operator(self, other_value):
        """results = False
        for vlm in self.value_level_metadata:
            results |= self.validation_df.apply(lambda row: vlm["filter"](row) and not vlm["type_check"](row), axis=1)
        return self.validation_df.convert_to_series(results.values)"""
        raise NotImplementedError("non_conformant_value_data_type check_operator not implemented")
