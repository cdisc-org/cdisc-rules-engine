from cdisc_rules_engine.rule_operators.dataframe_operators import DataframeType
from typing import Union
import dask.dataframe as dd
import dask.array as da
import numpy as np


class DaskDataframeType(DataframeType):
    name = "dask"

    def get_comparator_data(
        self, comparator, value_is_literal: bool = False
    ) -> Union[str, int]:
        if value_is_literal:
            return comparator
        elif comparator in self.value.columns:
            return self.value[comparator]
        else:
            return comparator

    def _get_string_part_series(self, part_to_validate: str, length: int, target: str):
        if not self.value[target].apply(type).eq(str).all().compute():
            raise ValueError("The operator can't be used with non-string values")

        if part_to_validate == "suffix":
            series_to_validate = self.value[target].str.slice(-length)
        elif part_to_validate == "prefix":
            series_to_validate = self.value[target].str.slice(stop=length)
        else:
            raise ValueError(
                f"Invalid part to validate: {part_to_validate}. \
                  Valid values are: suffix, prefix"
            )

        return series_to_validate

    def _convert_to_series(self, result):
        if self._is_series(result):
            return result
        elif isinstance(result, np.ndarray):
            return dd.from_array(result)
        return dd.from_array(da.from_array(result))

    def _to_numeric(self, target, **kwargs):
        return dd.to_numeric(target, **kwargs)

    def _is_series(self, data):
        return isinstance(data, dd.Series)

    def _where_less_than(self, target, comparison):
        return target.lt(comparison, fill_value=0)

    def _where_less_than_or_equal_to(self, target, comparison):
        return target.le(comparison, fill_value=0)

    def _where_greater_than(self, target, comparison):
        return target.gt(comparison, fill_value=0)

    def _get_series_values(self, series):
        return series.values.compute()

    def _where_greater_than_or_equal_to(self, target, comparison):
        return target.ge(comparison, fill_value=0)

    def _series_is_in(self, target, comparison_data):
        return comparison_data.isin(target.values.compute())

    def validate_series_length(self, data, target: str, min_length: int):
        value_counts = data[target].value_counts().to_dict()
        return data[target].apply(lambda x: value_counts.get(x, 0) > min_length)

    def present_on_multiple_rows_within(self, other_value: dict):
        """
        The operator ensures that the target is present on multiple rows
        within a group_by column. The dataframe is grouped by a certain column
        and the check is applied to each group.
        """

        target = self.replace_prefix(other_value.get("target"))
        min_count: int = other_value.get("comparator") or 1
        group_by_column = self.replace_prefix(other_value.get("within"))
        grouped = self.value.groupby(group_by_column)
        results = grouped.apply(
            lambda x: self.validate_series_length(x, target, min_count),
            meta=(target, self.value[target].dtype),
        )
        return results.reset_index(drop=True)

    def is_not_unique_relationship(self, other_value):
        """
        Validates one-to-one relationship between two columns
        (target and comparator) against a dataset.
        One-to-one means that a pair of columns can be duplicated
        but its integrity must not be violated:
        one value of target always corresponds
        to one value of comparator. Examples:

        Valid dataset:
        STUDYID  STUDYDESC
        1        A
        2        B
        3        C
        1        A
        2        B

        Invalid dataset:
        STUDYID  STUDYDESC
        1        A
        2        A
        3        C
        """
        target = other_value.get("target")
        comparator = other_value.get("comparator")
        # remove repeating rows
        df_without_duplicates = self.value[[target, comparator]].drop_duplicates()
        # we need to check if ANY of the columns (target or comparator) is duplicated
        for col in [target, comparator]:
            df = df_without_duplicates[col].value_counts().map(lambda x: x > 1)
            if True in df:
                return self._convert_to_series([True] * len(self.value))
        return self._convert_to_series([False] * len(self.value))
