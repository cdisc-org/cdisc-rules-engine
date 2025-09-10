from .base_sql_operator import BaseSqlOperator

COMPLETE_DATE_REGEX = "^\\d{4}-\\d{2}-\\d{2}(T\\d{2}:\\d{2}(:\\d{2})?(\\.\\d+)?([+-]\\d{2}:?\\d{2}|Z)?)?$"


class IsCompleteDateOperator(BaseSqlOperator):
    """Operator for checking if date is complete."""

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target"))
        target_sql = self._column_sql(target)
        op_name = f"{target}_is_complete_date"
        return self._do_check_operator(
            op_name,
            lambda: (
                f"""NOT ({self._is_empty_sql(target)})
                AND {target_sql}::text ~ '{COMPLETE_DATE_REGEX}'"""
            ),
        )
