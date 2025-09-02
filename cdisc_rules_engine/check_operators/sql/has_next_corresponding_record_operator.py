from .base_sql_operator import BaseSqlOperator
from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface


class HasNextCorrespondingRecordOperator(BaseSqlOperator):
    """Operator for checking if record has next corresponding record."""

    def execute_operator(self, other_value):
        """
        The operator ensures that value of target in current row
        is the same as value of comparator in the next row.
        In order to achieve this, we just remove last row from target
        and first row from comparator and compare the resulting contents.
        The result is reported for target.
        """
        """target = self.replace_prefix(other_value.get("target"))
        comparator = self.replace_prefix(other_value.get("comparator"))
        group_by_column: str = self.replace_prefix(other_value.get("within"))
        order_by_column: str = self.replace_prefix(other_value.get("ordering"))
        target_columns = [target, comparator, group_by_column, order_by_column]
        ordered_df = self.validation_df[target_columns].sort_values(by=[order_by_column])
        grouped_df = ordered_df.groupby(group_by_column)
        results = grouped_df.apply(lambda x: self.compare_target_with_comparator_next_row(x, target, comparator))
        return self.validation_df.convert_to_series(results.explode().tolist())"""
        raise NotImplementedError("has_next_corresponding_record check_operator not implemented")

    def compare_target_with_comparator_next_row(self, df: DatasetInterface, target: str, comparator: str):
        """
        Compares current row of a target with the next row of comparator.
        We can't
        compare last row of target with the next row of comparator
        because there is no row after the last one.
        """
        """target_without_last_row = df[target].drop(df[target].tail(1).index)
        comparator_without_first_row = df[comparator].drop(df[comparator].head(1).index)
        results = np.where(
            target_without_last_row.values == comparator_without_first_row.values,
            True,
            False,
        )
        # we add True at the end as the last row of target has nothing to compare
        # so as to not raise errors or incorrect issues in the report with False or NaN
        return self.validation_df.convert_to_series(
            [
                *results,
                True,
            ]
        ).tolist()"""
        raise NotImplementedError("compare_target_with_comparator_next_row check_operator not implemented")
