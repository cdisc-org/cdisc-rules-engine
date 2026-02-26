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

        Returns:
            Boolean series indicating if each record meets the sorting condition
        """
        target = self.replace_prefix(other_value.get("target"))
        within = self.replace_prefix(other_value.get("within"))
        comparators = other_value["comparator"]

        if not all([target, within, comparators]):
            raise ValueError("Missing required parameters: target, within, or comparator")

        comparator_parts = []
        for comp in comparators:
            name = self.replace_prefix(comp["name"])
            order = comp["sort_order"].upper()
            null_pos = comp["null_position"]
            comparator_parts.append(f"{name}_{order}_{null_pos}")

        cache_key = f"{target}_is_sorted_by_{'_'.join(comparator_parts)}_within_{within}"

        def sql(table_name, column_name):

            # Build CTEs for each individual comparator check
            comparator_ctes = []
            comparator_columns = []

            for i, comp in enumerate(comparators):
                comp_name = self.replace_prefix(comp["name"])
                comp_sql = self._column_sql(comp_name, alias=False)
                sort_order = comp["sort_order"].upper()
                null_pos = comp["null_position"].upper()

                # Build ORDER BY for this specific comparator
                order_part = f"{comp_sql} {sort_order}"
                if null_pos == "FIRST":
                    order_part += " NULLS FIRST"
                else:
                    order_part += " NULLS LAST"

                comparator_columns.append(f"{comp_sql} AS comp_{i}_val")

                # Create a CTE that checks if row positions match when sorted by comparator vs target
                comparator_ctes.append(
                    f"""
            comp_{i}_presorted AS (
                SELECT
                    id,
                    {self._column_sql(target, alias=False)} AS target_val,
                    {self._column_sql(within, alias=False)} AS within_val,
                    {comp_sql} AS comp_val,
                    ROW_NUMBER() OVER (
                        PARTITION BY {self._column_sql(within, alias=False)}
                        ORDER BY {order_part}
                    ) - 1 AS position_in_comp_order
                FROM {table_name}
            ),
            comp_{i}_sorted AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY within_val
                        ORDER BY
                            CASE WHEN target_val IS NULL THEN 1 ELSE 0 END,
                            target_val ASC
                    ) - 1 AS position_in_target_order
                FROM comp_{i}_presorted
            ),
            comp_{i}_check AS (
                SELECT
                    id,
                    CASE
                        -- If comparator is NULL, mark as False (old engine sets is_sorted[index]=False)
                        WHEN comp_val IS NULL THEN false
                        -- Check if positions match
                        ELSE position_in_comp_order = position_in_target_order
                    END AS is_valid_{i}
                FROM comp_{i}_sorted
            )"""
                )

            all_ctes = ",".join(comparator_ctes)

            # Build the join to combine all comparator checks
            join_parts = ["comp_0_check c0"]
            validity_checks = ["c0.is_valid_0"]

            for i in range(1, len(comparators)):
                join_parts.append(f"JOIN comp_{i}_check c{i} ON c0.id = c{i}.id")
                validity_checks.append(f"c{i}.is_valid_{i}")

            join_clause = "\n                ".join(join_parts)
            validity_clause = " AND ".join(validity_checks)

            # Also need the combined sort for date overlap check
            order_by_parts = []
            for i, comp in enumerate(comparators):
                comp_name = self.replace_prefix(comp["name"])
                sort_order = comp["sort_order"].upper()
                null_pos = comp["null_position"].upper()

                comp_sql = self._column_sql(comp_name, alias=False)
                order_part = f"{comp_sql} {sort_order}"
                if null_pos == "FIRST":
                    order_part += " NULLS FIRST"
                else:
                    order_part += " NULLS LAST"
                order_by_parts.append(order_part)

            order_by_clause = ", ".join(order_by_parts)
            comparator_columns_sql = ", ".join(comparator_columns)

            return f"""
            -- Check if target is sorted correctly by each comparator independently (matches old engine)
            WITH {all_ctes},
            basic_check AS (
                SELECT
                    c0.id,
                    ({validity_clause}) AS is_valid
                FROM {join_clause}
            ),
            sorted_for_overlap AS (
                SELECT
                    id,
                    {self._column_sql(within, alias=False)} AS within_val,
                    {comparator_columns_sql},
                    ROW_NUMBER() OVER (
                        PARTITION BY {self._column_sql(within, alias=False)}
                        ORDER BY {order_by_clause}
                    ) AS row_position
                FROM {table_name}
            ),
            date_overlap_check AS (
                SELECT
                    s1.id,
                    CASE
                        -- Use invalid_date operator logic to check if dates are valid before checking overlaps
                        WHEN ({self._is_invalid_date_sql("s1.comp_0_val")}) OR
                             ({self._is_invalid_date_sql("s2.comp_0_val")}) THEN true
                        WHEN s1.comp_0_val ~ '^[0-9]{{4}}$' AND s2.comp_0_val ~ '^[0-9]{{4}}-[0-9]{{2}}'
                             AND s2.comp_0_val LIKE s1.comp_0_val || '%' THEN false
                        WHEN s1.comp_0_val ~ '^[0-9]{{4}}-[0-9]{{2}}$'
                             AND s2.comp_0_val ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                             AND s2.comp_0_val LIKE s1.comp_0_val || '%' THEN false
                        WHEN s2.comp_0_val ~ '^[0-9]{{4}}$' AND s1.comp_0_val ~ '^[0-9]{{4}}-[0-9]{{2}}'
                             AND s1.comp_0_val LIKE s2.comp_0_val || '%' THEN false
                        WHEN s2.comp_0_val ~ '^[0-9]{{4}}-[0-9]{{2}}$'
                             AND s1.comp_0_val ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                             AND s1.comp_0_val LIKE s2.comp_0_val || '%' THEN false
                        ELSE true
                    END AS date_overlap_ok
                FROM sorted_for_overlap s1
                LEFT JOIN sorted_for_overlap s2 ON s1.within_val = s2.within_val
                    AND s2.row_position = s1.row_position + 1
            )
            UPDATE {table_name} t
            SET {column_name} = (
                SELECT (b.is_valid AND d.date_overlap_ok)
                FROM basic_check b
                JOIN date_overlap_check d ON b.id = d.id
                WHERE b.id = t.id
            )
            """

        return self._do_complex_check_operator(cache_key, sql)
