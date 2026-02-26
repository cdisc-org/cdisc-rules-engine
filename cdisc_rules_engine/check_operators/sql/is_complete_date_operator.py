from .base_sql_operator import BaseSqlOperator

COMPLETE_DATE_REGEX = "^\\d{4}-\\d{2}-\\d{2}(T\\d{2}:\\d{2}(:\\d{2})?(\\.\\d+)?([+-]\\d{2}:?\\d{2}|Z)?)?$"


class IsCompleteDateOperator(BaseSqlOperator):
    """Operator for checking if date is complete."""

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target"))

        def sql():
            return f"""CASE WHEN
                NOT ({self._is_empty_sql(target)})
                AND {self._column_sql(target)} ~ '{COMPLETE_DATE_REGEX}'
                THEN true
                ELSE false
                END"""

        return self._do_check_operator(sql)
