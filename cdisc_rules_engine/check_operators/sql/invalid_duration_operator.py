from .base_sql_operator import BaseSqlOperator


class InvalidDurationOperator(BaseSqlOperator):
    """Operator for checking if duration is invalid."""

    def execute_operator(self, other_value):
        """target = self.replace_prefix(other_value.get("target"))
        if other_value.get("negative") is False:
            results = ~vectorized_is_valid_duration(self.validation_df[target], False)
        else:
            results = ~vectorized_is_valid_duration(self.validation_df[target], True)
        return self.validation_df.convert_to_series(results)"""
        raise NotImplementedError("invalid_duration check_operator not implemented")
