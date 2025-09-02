from .base_sql_operator import BaseSqlOperator


class IsCompleteDateOperator(BaseSqlOperator):
    """Operator for checking if date is complete."""

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target")).lower()
        op_name = f"{target}_is_complete_date"
        return self._do_check_operator(
            op_name,
            lambda: (
                f"CASE WHEN {target} IS NOT NULL "
                f"AND {target} != '' "
                f"AND {target}::text ~ "
                f"'^\\d{{4}}-\\d{{2}}-\\d{{2}}"
                f"(T\\d{{2}}:\\d{{2}}(:\\d{{2}})?(\\.\\d+)?([+-]\\d{{2}}:?\\d{{2}}|Z)?)?$' "
                f"THEN TRUE ELSE FALSE END"
            ),
        )
