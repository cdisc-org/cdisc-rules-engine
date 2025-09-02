from .base_sql_operator import BaseSqlOperator


class IsInconsistentAcrossDatasetOperator(BaseSqlOperator):
    """Operator for checking if values are inconsistent across dataset."""

    def execute_operator(self, other_value):
        """target = self.replace_prefix(other_value.get("target"))
        comparator = other_value.get("comparator")
        grouping_cols = []
        if isinstance(comparator, str):
            col_name = self.replace_prefix(comparator)
            if col_name in self.validation_df.columns:
                grouping_cols.append(col_name)
        else:
            for col in comparator:
                col_name = self.replace_prefix(col)
                if col_name in self.validation_df.columns:
                    grouping_cols.append(col_name)
        df_check = self.validation_df[grouping_cols + [target]].copy()
        df_check = df_check.fillna("_NaN_")
        results = pd.Series(True, index=df_check.index)
        for name, group in df_check.groupby(grouping_cols):
            if group[target].nunique() == 1:
                results[group.index] = False
        return results"""
        raise NotImplementedError("is_inconsistent_across_dataset check_operator not implemented")
