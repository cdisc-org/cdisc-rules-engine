from cdisc_rules_engine.utilities.utils import flatten_nested_list

from .base_sql_operator import BaseSqlOperator


class IsUniqueSetOperator(BaseSqlOperator):
    """
    Operator for checking if values form a unique set.
    This operator validates that the combination of target and comparator columns
    forms a unique set, which means no duplicate combinations exist in the dataset.
    Each unique combination should appear at most once.
    """

    def execute_operator(self, other_value):
        target = other_value.get("target")
        comparator = other_value.get("comparator")

        all_columns = flatten_nested_list([target, comparator])

        seen = set()
        unique_columns = []

        for col_raw in all_columns:
            clean_name = self.replace_prefix(col_raw).lower()
            clean_col = clean_name if self._exists(clean_name) else None
            if clean_col and clean_col not in seen:
                seen.add(clean_col)
                unique_columns.append(clean_col)

        if not unique_columns:
            raise ValueError("No valid columns found for uniqueness check.")

        def key_of(alias: bool) -> str:
            parts = [
                f"COALESCE(NULLIF(CAST({self._column_sql(col, alias=alias)} AS TEXT), ''), '_NULL_')"
                for col in unique_columns
            ]
            return " || '|' || ".join(parts)

        # we're grouping once here then joining the duplicated keys back on rather than COUNT(*) OVER per row, which
        # needed a full sort of the dataset inside the source subquery.
        # crazy run-time jump if we go the other way.
        repeated_keys = f"SELECT {key_of(alias=False)} AS key FROM {self._table_sql()} GROUP BY 1 HAVING COUNT(*) > 1"
        repeated = self._join_derived(repeated_keys, lambda joined: f"{joined}.key = {key_of(alias=True)}")
        return self._do_check_operator(lambda: f"{repeated}.key IS NULL")
