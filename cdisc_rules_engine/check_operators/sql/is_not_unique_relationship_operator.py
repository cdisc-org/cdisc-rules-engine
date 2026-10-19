from .base_sql_operator import BaseSqlOperator


class IsNotUniqueRelationshipOperator(BaseSqlOperator):
    """Operator for checking non-unique relationships between columns."""

    def execute_operator(self, other_value):
        """
        Validates one-to-one relationship between
        two columns (target and comparator) against a dataset.
        One-to-one means that a pair of columns can be duplicated
        but its integrity must not be violated:
        one value of target always corresponds to
        one value of comparator.
        Examples:

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
        target_column = self.replace_prefix(other_value.get("target"))
        if not self._exists(target_column):
            return self._do_check_operator(lambda: "FALSE")
        target = self._column_sql(target_column, alias=False)
        comparator = other_value.get("comparator")

        if isinstance(comparator, list):
            comparator_columns = [self.replace_prefix(col) for col in comparator]
            comparator_sql = [self._column_sql(col, alias=False) for col in comparator_columns if self._exists(col)]
            if not comparator_sql:
                return self._do_check_operator(lambda: "FALSE")
            comparator_list = ", ".join([f"COALESCE({c}::text, '') AS {c}" for c in comparator_sql])
            concat_expr = " || '|' || ".join(
                f"COALESCE({comparator_columns}::text, '')" for comparator_columns in comparator_sql
            )
            op_name = f"{target}_{'_'.join(comparator_columns)}_not_unique_relationship"
        else:
            comparator_column = self.replace_prefix(comparator)
            if not self._exists(comparator_column):
                return self._do_check_operator(lambda: "FALSE")
            comparator_sql = self._column_sql(comparator_column, alias=False)
            comparator_list = f"COALESCE({comparator_sql}::text, '') AS {comparator_sql}"
            concat_expr = f"COALESCE({comparator_sql}::text, '')"
            op_name = f"{target}_{comparator_column}_not_unique_relationship"

        target_sql = f"COALESCE({target}::text, '')"

        violations = self._violations_join(
            target_column, other_value.get("comparator"), target, target_sql, comparator_list, concat_expr
        )
        if violations:
            return self._do_check_operator(lambda: violations)

        def generate_update_query(db_table: str, db_column: str) -> str:
            return f"""
                UPDATE {db_table} AS t
                SET {db_column} = sub.has_violation
                FROM (
                    WITH distinct_pairs AS (
                        SELECT DISTINCT {target_sql} AS {target}, {comparator_list}
                        FROM {db_table}
                    ),
                    target_violations AS (
                        SELECT {target}
                        FROM distinct_pairs
                        GROUP BY {target}
                        HAVING COUNT(DISTINCT {concat_expr}) > 1
                    ),
                    comparator_violations AS (
                        SELECT {concat_expr} as comp_key
                        FROM distinct_pairs
                        GROUP BY {concat_expr}
                        HAVING COUNT(DISTINCT {target}) > 1
                    )
                    SELECT
                        id,
                        CASE WHEN
                            {target_sql} IN (SELECT {target} FROM target_violations) OR
                            {concat_expr} IN (SELECT comp_key FROM comparator_violations)
                        THEN true ELSE false END AS has_violation
                    FROM {db_table}
                    ORDER BY id
                ) AS sub
                WHERE t.id = sub.id;
            """

        return self._do_complex_check_operator(op_name, generate_update_query)

    def _violations_join(self, target_column, comparator, target, target_sql, comparator_list, concat_expr):
        """Condition for a row violating the relationship with its violations joined in."""
        pairs = (
            f"WITH distinct_pairs AS (SELECT DISTINCT {target_sql} AS {target}, {comparator_list} "
            f"FROM {self._table_sql()})"
        )
        violating_targets = (
            f"{pairs} SELECT {target} AS key FROM distinct_pairs "
            f"GROUP BY {target} HAVING COUNT(DISTINCT {concat_expr}) > 1"
        )
        violating_comparators = (
            f"{pairs} SELECT {concat_expr} AS key FROM distinct_pairs "
            f"GROUP BY {concat_expr} HAVING COUNT(DISTINCT {target}) > 1"
        )

        row_target = f"COALESCE({self._column_sql(target_column)}::text, '')"
        comparator_columns = comparator if isinstance(comparator, list) else [comparator]
        row_comparator = " || '|' || ".join(
            f"COALESCE({self._column_sql(self.replace_prefix(column))}::text, '')"
            for column in comparator_columns
            if self._exists(self.replace_prefix(column))
        )

        by_target = self._join_derived(violating_targets, lambda alias: f"{alias}.key = {row_target}")
        if by_target is None:
            return None
        by_comparator = self._join_derived(violating_comparators, lambda alias: f"{alias}.key = {row_comparator}")
        return f"{by_target}.key IS NOT NULL OR {by_comparator}.key IS NOT NULL"

    def has_missing_required_columns(self, other_value) -> bool:
        """Whether execute_operator would fall back to its default FALSE result."""
        target_column = self.replace_prefix(other_value.get("target"))
        if not self._exists(target_column):
            return True

        comparator = other_value.get("comparator")
        if isinstance(comparator, list):
            comparator_columns = [self.replace_prefix(col) for col in comparator]
            return not any(self._exists(col) for col in comparator_columns)
        else:
            comparator_column = self.replace_prefix(comparator)
            return not self._exists(comparator_column)
