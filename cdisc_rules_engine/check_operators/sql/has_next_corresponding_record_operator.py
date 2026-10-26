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

        window = f"PARTITION BY {self._source_column_sql(group_by)} ORDER BY {self._source_column_sql(order_by)}"
        partition = f"PARTITION BY {self._source_column_sql(group_by)}"
        next_comparator = f"LEAD({self._source_column_sql(comparator)}) OVER ({window})"

        expression = f"""CASE
            WHEN ROW_NUMBER() OVER ({window}) = COUNT(*) OVER ({partition}) THEN TRUE
            WHEN {next_comparator} IS NULL THEN FALSE
            WHEN {self._source_column_sql(target)} = {next_comparator} THEN TRUE
            ELSE FALSE
        END"""
        return self._window_column(cache_key, f"({expression})", column_type="Bool")
