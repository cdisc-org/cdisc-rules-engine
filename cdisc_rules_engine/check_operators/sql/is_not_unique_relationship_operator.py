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
        target_column = self.replace_prefix(other_value.get("target")).lower()
        comparator = other_value.get("comparator")

        if isinstance(comparator, list):
            comparator_columns = [self.replace_prefix(col).lower() for col in comparator]
            comparator_list = ", ".join(comparator_columns)
            concat_expr = " || '|' || ".join(comparator_columns)
            op_name = f"{target_column}_{'_'.join(comparator_columns)}_not_unique_relationship"
        else:
            comparator_column = self.replace_prefix(comparator).lower()
            comparator_list = comparator_column
            concat_expr = comparator_column
            op_name = f"{target_column}_{comparator_column}_not_unique_relationship"

        def generate_update_query(db_table: str, db_column: str) -> str:
            return f"""
                UPDATE {db_table} AS t
                SET {db_column} = sub.has_violation
                FROM (
                    WITH distinct_pairs AS (
                        SELECT DISTINCT {target_column}, {comparator_list}
                        FROM {db_table}
                    ),
                    target_violations AS (
                        SELECT {target_column}
                        FROM distinct_pairs
                        GROUP BY {target_column}
                        HAVING COUNT(DISTINCT {concat_expr}) > 1
                    ),
                    comparator_violations AS (
                        SELECT {concat_expr} as comp_key
                        FROM distinct_pairs
                        GROUP BY {concat_expr}
                        HAVING COUNT(DISTINCT {target_column}) > 1
                    )
                    SELECT
                        id,
                        CASE WHEN
                            {target_column} IN (SELECT {target_column} FROM target_violations) OR
                            {concat_expr} IN (SELECT comp_key FROM comparator_violations)
                        THEN true ELSE false END AS has_violation
                    FROM {db_table}
                    ORDER BY id
                ) AS sub
                WHERE t.id = sub.id;
            """

        return self._do_complex_check_operator(op_name, generate_update_query)
