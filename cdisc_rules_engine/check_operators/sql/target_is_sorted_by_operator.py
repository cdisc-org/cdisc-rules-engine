from cdisc_rules_engine.compiler.derived import SOURCE_ALIAS

from .base_sql_operator import BaseSqlOperator


class TargetIsSortedByOperator(BaseSqlOperator):
    """Operator for checking if target is sorted by specified criteria."""

    def _is_invalid_date_sql(self, date_column):
        """
        Check if a date is invalid using simple SQL logic.
        Returns SQL expression that evaluates to TRUE if the date is invalid, FALSE if valid.
        """
        return f"""NOT (
                -- Valid ISO 8601 formats
                {date_column} ~ '^[0-9]{{4}}$' OR
                {date_column} ~ '^[0-9]{{4}}-[0-9]{{2}}$' OR
                {date_column} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' OR
                {date_column} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}$' OR
                {date_column} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$' OR
                {date_column} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}$' OR
                {date_column} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$' OR
                -- Uncertainty patterns
                {date_column} ~ '^[0-9]{{4}}--$' OR
                {date_column} ~ '^[0-9]{{4}}-[0-9]{{2}}--$' OR
                {date_column} ~ '^[0-9]{{4}}----$'
            )"""

    def execute_operator(self, other_value):
        """
        Checks if target values are sorted correctly based on comparator columns.

        For each group (defined by 'within'), verifies that target values follow
        the expected order when rows are sorted by comparator columns. Also handles
        date overlap validation for partial dates.

        Args:
            other_value: Dictionary containing:
                - target: The target column to check sorting of
                - within: The column to group by
                - comparator: List of dictionaries with:
                    - name: Column name to sort by
                    - sort_order: "ASC" or "DESC"
                    - null_position: "first" or "last"
                - strict_incremental_ordering: (optional, default False) If True,
                    checks that target values equal their 1-based row position within
                    each group when ordered by comparator (e.g. 1 2 3 or 01 02 03).

        Returns:
            Boolean series indicating if each record meets the sorting condition
        """
        regex_sort = other_value.get("regex", None)
        target = self.replace_prefix(other_value.get("target"))
        val = other_value.get("within")
        within = val if isinstance(val, list) else [val]
        within = [self.replace_prefix(val) for val in within]
        comparators = other_value["comparator"]
        strict_incremental = other_value.get("strict_incremental_ordering", False)

        if not all([target, within, comparators]):
            raise ValueError("Missing required parameters: target, within, or comparator")

        cache_key = self._cache_key(target, within, comparators, strict_incremental)

        partition = ", ".join(self._source_column_sql(column) for column in within)
        target_sql = self._source_column_sql(target)
        if regex_sort:
            target_sql = f"CAST((regexp_match({target_sql}, '{regex_sort}'))[1] AS INTEGER)"

        if strict_incremental:
            expression = self._strict_incremental_sql(target_sql, partition, comparators)
        else:
            expression = (
                f"(({self._positions_agree_sql(target_sql, partition, comparators)})"
                f" AND ({self._date_overlap_sql(partition, comparators, within)}))"
            )
        return self._window_column(cache_key, expression, column_type="Bool")

    def _cache_key(self, target, within, comparators, strict_incremental) -> str:
        comparator_parts = [
            f"{self.replace_prefix(comp['name'])}_{comp['sort_order'].upper()}_{comp['null_position']}"
            for comp in comparators
        ]
        strict_suffix = "_strict_incremental" if strict_incremental else ""
        within_key = "_".join(within)
        return f"{target}_is_sorted_by_{'_'.join(comparator_parts)}_within_{within_key}{strict_suffix}"

    def _order_part(self, comparator) -> str:
        column = self._source_column_sql(self.replace_prefix(comparator["name"]))
        nulls = "NULLS FIRST" if comparator["null_position"].upper() == "FIRST" else "NULLS LAST"
        return f"{column} {comparator['sort_order'].upper()} {nulls}"

    def _strict_incremental_sql(self, target_sql: str, partition: str, comparators) -> str:
        ordering = ", ".join(self._order_part(comparator) for comparator in comparators)
        position = f"ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {ordering})"
        return f"(CASE WHEN {target_sql} IS NULL THEN TRUE" f" ELSE CAST({target_sql} AS INTEGER) = {position} END)"

    def _positions_agree_sql(self, target_sql: str, partition: str, comparators) -> str:
        """
        Each row's position ordered by a comparator must match its position ordered by the target.
        Ties are broken as target's ties by the comparators and visa versa. Then by the row.
        """
        tie_break = ", ".join(self._order_part(comparator) for comparator in comparators)
        target_order = f"CASE WHEN {target_sql} IS NULL THEN 1 ELSE 0 END, {target_sql} ASC"
        row = f"{SOURCE_ALIAS}.id"
        by_target = f"ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {target_order}, {tie_break}, {row})"
        checks = []
        for comparator in comparators:
            column = self._source_column_sql(self.replace_prefix(comparator["name"]))
            by_comparator = (
                f"ROW_NUMBER() OVER (PARTITION BY {partition} "
                f"ORDER BY {self._order_part(comparator)}, {target_order}, {row})"
            )
            checks.append(f"(CASE WHEN {column} IS NULL THEN FALSE ELSE {by_comparator} = {by_target} END)")
        return " AND ".join(checks)

    def _date_overlap_sql(self, partition: str, comparators, within) -> str:
        """
        A partial date must not contain the next row's date.
        "2020" followed by "2020-01" is an overlap instead of a proper chronological ordering.
        LEAD supplies the next row.
        A row whose group has a missing value has no next row to overlap as rows
        are only paired up when their groups are equal.
        """
        any_within_missing = " OR ".join(f"{self._source_column_sql(column)} IS NULL" for column in within)
        first = self._source_column_sql(self.replace_prefix(comparators[0]["name"]))
        ordering = ", ".join(self._order_part(comparator) for comparator in comparators)
        nxt = f"LEAD({first}) OVER (PARTITION BY {partition} ORDER BY {ordering})"

        year = r"'^[0-9]{4}$'"
        year_month = r"'^[0-9]{4}-[0-9]{2}$'"
        into_month = r"'^[0-9]{4}-[0-9]{2}'"
        into_day = r"'^[0-9]{4}-[0-9]{2}-[0-9]{2}'"

        return f"""CASE
            WHEN {any_within_missing} THEN TRUE
            WHEN ({self._is_invalid_date_sql(first)}) OR ({self._is_invalid_date_sql(nxt)}) THEN TRUE
            WHEN {first} ~ {year} AND {nxt} ~ {into_month}
                 AND {nxt} LIKE {first} || '%' THEN FALSE
            WHEN {first} ~ {year_month} AND {nxt} ~ {into_day}
                 AND {nxt} LIKE {first} || '%' THEN FALSE
            WHEN {nxt} ~ {year} AND {first} ~ {into_month}
                 AND {first} LIKE {nxt} || '%' THEN FALSE
            WHEN {nxt} ~ {year_month} AND {first} ~ {into_day}
                 AND {first} LIKE {nxt} || '%' THEN FALSE
            ELSE TRUE
        END"""
