from .base_sql_operator import BaseSqlOperator


class HasNextCorrespondingRecordOperator(BaseSqlOperator):
    """Operator for checking if record has next corresponding record."""

    def execute_operator(self, other_value):
        """
        Checks if the current record's target value matches the next record's comparator value.

        For each record, checks if its target column value equals the comparator column value
        of the next record in the same group (defined by the 'within' parameter) when ordered
        by the 'ordering' parameter.

        Args:
            other_value: Dictionary containing:
                - target: The target column to check
                - comparator: The column to compare with in the next row
                - within: The column to group by
                - ordering: The column to order by within each group

        Returns:
            Boolean series indicating if each record meets the condition
        """
        target = self.replace_prefix(other_value.get("target"))
        comparator = self.replace_prefix(other_value.get("comparator"))
        group_by = self.replace_prefix(other_value.get("within"))
        order_by = self.replace_prefix(other_value.get("ordering"))

        if not all([target, comparator, group_by, order_by]):
            raise ValueError("Missing required parameters: target, comparator, within, or ordering")

        cache_key = f"{target}_has_next_corresponding_record_{comparator}_within_" f"{group_by}_ordered_by_{order_by}"

        def sql(table_name, column_name):
            return f"""
            -- For each row, compare current target to next row's comparator; last row in group is always True
            WITH ranked AS (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY {self._column_sql(group_by)}
                        ORDER BY {self._column_sql(order_by)}
                    ) AS rn,
                    COUNT(*) OVER (
                        PARTITION BY {self._column_sql(group_by)}
                    ) AS cnt,
                    {self._column_sql(target)} AS target_val,
                    LEAD({self._column_sql(comparator)}) OVER (
                        PARTITION BY {self._column_sql(group_by)}
                        ORDER BY {self._column_sql(order_by)}
                    ) AS next_comp
                FROM {table_name}
            )
            UPDATE {table_name} t
            SET {column_name} = (
                SELECT CASE
                    WHEN r.rn = r.cnt THEN true
                    WHEN r.next_comp IS NULL THEN false
                    WHEN r.target_val = r.next_comp THEN true
                    ELSE false
                END
                FROM ranked r
                WHERE r.id = t.id
            )
            """

        return self._do_complex_check_operator(cache_key, sql)
