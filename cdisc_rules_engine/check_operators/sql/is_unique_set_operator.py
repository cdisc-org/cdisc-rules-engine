from .base_sql_operator import BaseSqlOperator
from cdisc_rules_engine.utilities.utils import flatten_nested_list


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

        op_name = f"{'_'.join(unique_columns)}_is_unique_set"

        def generate_update_query(db_table: str, db_column: str) -> str:
            concat_parts = [
                f"COALESCE(NULLIF(CAST({self._column_sql(col)} AS TEXT), ''), '_NULL_')" for col in unique_columns
            ]
            concat_expr = " || '|' || ".join(concat_parts)

            return f"""
                UPDATE {db_table} AS t
                SET {db_column} = sub.is_unique
                FROM (
                    SELECT
                        id,
                        CASE
                            WHEN COUNT(*) OVER (
                                PARTITION BY {concat_expr}
                            ) <= 1
                            THEN TRUE
                            ELSE FALSE
                        END AS is_unique
                    FROM {db_table}
                    ORDER BY id
                ) AS sub
                WHERE t.id = sub.id;
            """

        return self._do_complex_check_operator(op_name, generate_update_query)
