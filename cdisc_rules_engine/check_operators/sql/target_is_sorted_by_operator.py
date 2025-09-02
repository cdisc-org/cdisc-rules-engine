import pandas as pd
from .base_sql_operator import BaseSqlOperator
from business_rules.utils import (
    is_valid_date,
)
from cdisc_rules_engine.utilities.utils import dates_overlap, parse_date


class TargetIsSortedByOperator(BaseSqlOperator):
    """Operator for checking if target is sorted by specified criteria."""

    def execute_operator(self, other_value):
        """
        Checking the sort order based on comparators, including date overlap checks
        """
        """target: str = self.replace_prefix(other_value.get("target"))
        within: str = self.replace_prefix(other_value.get("within"))
        columns = other_value["comparator"]
        result = pd.Series([True] * len(self.validation_df), index=self.validation_df.index)
        pandas = isinstance(self.validation_df, PandasDataset)
        for col in columns:
            comparator: str = self.replace_prefix(col["name"])
            ascending: bool = col["sort_order"].lower() != "desc"
            na_pos: str = col["null_position"]
            sorted_df = self.validation_df[[target, within, comparator]].sort_values(
                by=[within, comparator], ascending=ascending, na_position=na_pos
            )
            grouped_df = sorted_df.groupby(within)

            # Check basic sort order, remove multiindex from series
            basic_sort_check = grouped_df.apply(lambda x: self.check_basic_sort_order(x, target, comparator, ascending))
            if pandas:
                basic_sort_check = basic_sort_check.reset_index(level=0, drop=True)
            else:
                basic_sort_check = basic_sort_check.reset_index(drop=True)
            result = result & basic_sort_check

            # Check date overlaps, remove multiindex from series
            date_overlap_check = grouped_df.apply(lambda x: self.check_date_overlaps(x, target, comparator))
            if pandas:
                date_overlap_check = date_overlap_check.reset_index(level=0, drop=True)
            else:
                date_overlap_check = date_overlap_check.reset_index(drop=True)
            result = result & date_overlap_check

            # handle edge case where a dataframe is returned
            if isinstance(result, (pd.DataFrame, dd.DataFrame)):
                if isinstance(result, dd.DataFrame):
                    result = result.compute()
                result = result.squeeze()
        return result"""
        raise NotImplementedError("target_is_sorted_by check_operator not implemented")

    def check_basic_sort_order(self, group, target, comparator, ascending):
        target_values = group[target].tolist()
        comparator_values = group[comparator].tolist()
        is_sorted = pd.Series(True, index=group.index)

        def safe_compare(x, index):
            if pd.isna(x):
                is_sorted[index] = False
                return "9999-12-31" if ascending else "0001-01-01"
            return x

        expected_order = sorted(
            range(len(comparator_values)),
            key=lambda k: safe_compare(comparator_values[k], group.index[k]),
            reverse=not ascending,
        )
        actual_order = sorted(range(len(target_values)), key=lambda k: target_values[k])

        for i, (exp, act) in enumerate(zip(expected_order, actual_order)):
            if exp != act:
                is_sorted.iloc[i] = False

        return is_sorted

    def check_date_overlaps(self, group, target, comparator):
        comparator_values = group[comparator].tolist()
        is_sorted = pd.Series(True, index=group.index)

        for i in range(len(comparator_values) - 1):
            if is_valid_date(comparator_values[i]) and is_valid_date(comparator_values[i + 1]):
                date1, prec1 = parse_date(comparator_values[i])
                date2, prec2 = parse_date(comparator_values[i + 1])
                if prec1 != prec2:
                    overlaps, less_precise = dates_overlap(date1, prec1, date2, prec2)
                    if overlaps and date1.startswith(less_precise):
                        is_sorted.iloc[i] = False
                    elif overlaps and date2.startswith(less_precise):
                        is_sorted.iloc[i + 1] = False

        return is_sorted
