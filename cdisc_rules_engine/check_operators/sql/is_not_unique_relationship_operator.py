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
        target = self._column_sql(target_column, alias=False)
        comparator = other_value.get("comparator")

        if isinstance(comparator, list):
            comparator_columns = [self.replace_prefix(col) for col in comparator]
            comparator_sql = [self._column_sql(col, alias=False) for col in comparator_columns]
            comparator_list = ", ".join([f"COALESCE({c}::text, '') AS {c}" for c in comparator_sql])
            concat_expr = " || '|' || ".join(
                f"COALESCE({comparator_columns}::text, '')" for comparator_columns in comparator_sql
            )
            op_name = f"{target}_{'_'.join(comparator_columns)}_not_unique_relationship"
        else:
            comparator_column = self.replace_prefix(comparator)
            comparator_sql = self._column_sql(comparator_column, alias=False)
            comparator_list = f"COALESCE({comparator_sql}::text, '') AS {comparator_sql}"
            concat_expr = f"COALESCE({comparator_sql}::text, '')"
            op_name = f"{target}_{comparator_column}_not_unique_relationship"

        target_sql = f"COALESCE({target}::text, '')"

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
