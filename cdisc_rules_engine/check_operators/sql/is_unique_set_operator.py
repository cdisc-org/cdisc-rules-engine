from .base_sql_operator import BaseSqlOperator


class IsUniqueSetOperator(BaseSqlOperator):
    """Operator for checking if values form a unique set."""

    def execute_operator(self, other_value):
        """target = self.replace_prefix(other_value.get("target"))
        comparator = other_value.get("comparator")
        values = [target, comparator]
        target_data = flatten_list(self.validation_df, values)
        target_names = []
        for target_name in target_data:
            target_name = self.replace_prefix(target_name)
            if target_name in self.validation_df.columns:
                target_names.append(target_name)
        target_names = list(set(target_names))
        df_group = self.validation_df[target_names].copy()
        df_group = df_group.fillna("_NaN_")
        group_sizes = df_group.groupby(target_names).size()
        counts = df_group.apply(tuple, axis=1).map(group_sizes)
        results = np.where(counts <= 1, True, False)
        return self.validation_df.convert_to_series(results)"""
        raise NotImplementedError("is_unique_set check_operator not implemented")
