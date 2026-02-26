from .base_sql_operator import BaseSqlOperator


class IsOrderedSubsetOfOperator(BaseSqlOperator):
    """Operator for checking if value is an ordered subset of another value."""

    def execute_operator(self, other_value):
        """target = self.replace_prefix(other_value.get("target"))
        comparator = self.replace_prefix(other_value.get("comparator"))
        missing_columns = set()

        def check_order(row):
            target_list = row[target]
            comparator_list = row[comparator]
            comparator_positions = {col: idx for idx, col in enumerate(comparator_list)}
            positions = []
            for col in target_list:
                if col in comparator_positions:
                    positions.append(comparator_positions[col])
                else:
                    missing_columns.add(col)
                    return False
            return positions == sorted(positions)

        if isinstance(self.validation_df, DaskDataset):
            results = self.validation_df.apply(check_order, axis=1, meta=("check_order", bool))
            results = self.validation_df.convert_to_series(results)
        else:
            results = self.validation_df.apply(check_order, axis=1)
        if missing_columns:
            logger.info(f"Columns not found in comparator list {comparator}: {', '.join(sorted(missing_columns))}")
        return results"""
        raise NotImplementedError("is_ordered_subset_of check_operator not implemented")
