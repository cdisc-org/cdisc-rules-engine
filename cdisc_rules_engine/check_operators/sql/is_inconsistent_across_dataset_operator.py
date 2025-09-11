from .base_sql_operator import BaseSqlOperator


class IsInconsistentAcrossDatasetOperator(BaseSqlOperator):
    """Operator for checking if values are inconsistent across dataset."""

    def execute_operator(self, other_value):
        """
        Checks if values in the target column are inconsistent across groups defined by comparator column(s).

        Returns True for rows where the target column has multiple distinct values within the same group,
        False for rows where all values in the group are consistent.
        """
        target_column = self.replace_prefix(other_value.get("target")).lower()
        comparator = other_value.get("comparator")

        if isinstance(comparator, str):
            return self._handle_single_comparator(target_column, comparator)
        elif isinstance(comparator, list):
            return self._handle_multiple_comparators(target_column, comparator)
        else:
            raise ValueError(
                f"Invalid comparator type for is_inconsistent_across_dataset operation on column '{target_column}'. "
                "Expected string or list of column names."
            )

    def _handle_single_comparator(self, target_column, comparator):
        """Handle when comparator is a single column name."""
        comparator_column = self.replace_prefix(comparator).lower()

        if not self._exists(comparator_column):
            raise ValueError(f"Comparator column '{comparator}' does not exist in the dataset.")

        cache_key = f"{target_column}_inconsistent_across_{comparator_column}"

        def generate_update_query(db_table: str, db_column: str) -> str:
            return f"""
                UPDATE {db_table} AS t
                SET {db_column} = sub.is_inconsistent
                FROM (
                    SELECT
                        id,
                        (
                            SELECT COUNT(DISTINCT
                                CASE
                                    WHEN t2.{self._column_sql(target_column)} IS NULL THEN 'NULL_VALUE'
                                    ELSE CAST(t2.{self._column_sql(target_column)} AS TEXT)
                                END
                            )
                            FROM {db_table} AS t2
                            WHERE (
                                (t2.{self._column_sql(comparator_column)} = t1.{self._column_sql(comparator_column)})
                                OR
                                (t2.{self._column_sql(comparator_column)} IS NULL
                                AND
                                t1.{self._column_sql(comparator_column)} IS NULL)
                            )
                        ) > 1 AS is_inconsistent
                    FROM {db_table} AS t1
                    ORDER BY id
                ) AS sub
                WHERE t.id = sub.id;
            """

        return self._do_complex_check_operator(cache_key, generate_update_query)

    def _handle_multiple_comparators(self, target_column, comparators):
        """Handle when comparator is a list of column names."""
        comparator_columns = []
        for comp in comparators:
            comp_col = self.replace_prefix(comp).lower()
            if not self._exists(comp_col):
                raise ValueError(f"Comparator column '{comp}' does not exist in the dataset.")
            comparator_columns.append(comp_col)

        cache_key = f"{target_column}_inconsistent_across_{'_'.join(comparator_columns)}"

        def generate_update_query(db_table: str, db_column: str) -> str:
            # Build the WHERE clause for matching groups, handling NULLs properly
            where_conditions = []
            for comp_col in comparator_columns:
                condition = (
                    f"(t2.{self._column_sql(comp_col)} = t1.{self._column_sql(comp_col)}) "
                    f"OR (t2.{self._column_sql(comp_col)} IS NULL AND t1.{self._column_sql(comp_col)} IS NULL)"
                )
                where_conditions.append(f"({condition})")
            where_clause = " AND ".join(where_conditions)

            return f"""
                UPDATE {db_table} AS t
                SET {db_column} = sub.is_inconsistent
                FROM (
                    SELECT
                        id,
                        (
                            SELECT COUNT(DISTINCT
                                CASE
                                    WHEN t2.{self._column_sql(target_column)} IS NULL THEN 'NULL_VALUE'
                                    ELSE CAST(t2.{self._column_sql(target_column)} AS TEXT)
                                END
                            )
                            FROM {db_table} AS t2
                            WHERE {where_clause}
                        ) > 1 AS is_inconsistent
                    FROM {db_table} AS t1
                    ORDER BY id
                ) AS sub
                WHERE t.id = sub.id;
            """

        return self._do_complex_check_operator(cache_key, generate_update_query)
