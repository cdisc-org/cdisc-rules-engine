from .base_sql_operator import BaseSqlOperator


class EmptyWithinExceptLastRowOperator(BaseSqlOperator):
    """Operator for checking if values are empty within group except last row."""

    def execute_operator(self, other_value):
        """target = self.replace_prefix(other_value.get("target"))
        comparator = other_value.get("comparator")
        order_by_column: str = self.replace_prefix(other_value.get("ordering"))
        # group all targets by comparator
        if order_by_column:
            ordered_df = self.validation_df.sort_values(by=[comparator, order_by_column])
        else:
            ordered_df = self.validation_df.sort_values(by=[comparator])
        grouped_target = ordered_df.groupby(comparator)[target]
        # validate all targets except the last one
        results = grouped_target.apply(lambda x: x[:-1]).apply(
            lambda x: (pd.isna(x).all() if isinstance(x, (pd.Series, list)) else (x in NULL_FLAVORS or pd.isna(x)))
        )
        if isinstance(self.validation_df, DaskDataset) and self.validation_df.is_series(results):
            results = results.compute()
        # return values with corresponding indexes from results
        return pd.Series(results.reset_index(level=0, drop=True))"""
        raise NotImplementedError("empty_within_except_last_row check_operator not implemented")
