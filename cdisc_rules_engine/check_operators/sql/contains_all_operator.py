from .base_sql_operator import BaseSqlOperator


class ContainsAllOperator(BaseSqlOperator):
    """Operator for checking if value contains all expected elements."""

    def execute_operator(self, other_value):
        """target = self.replace_prefix(other_value.get("target"))
        comparator = other_value.get("comparator")
        if isinstance(comparator, list):
            # get column as array of values
            values = flatten_list(self.validation_df, comparator)
        else:
            comparator = self.replace_prefix(comparator)
            values = self.validation_df[comparator].unique()
        return self.validation_df.convert_to_series(set(values).issubset(set(self.validation_df[target].unique())))"""
        raise NotImplementedError("contains_all check_operator not implemented")
