from .base_sql_operator import BaseSqlOperator


class NotMatchesRegexOperator(BaseSqlOperator):
    """Operator for inverted regex pattern matching."""

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target")).lower()
        target_column = self._column_sql(target)
        comparator = other_value.get("comparator")

        def sql():
            return f"""CASE WHEN
                            {target_column} IS NOT NULL
                            AND NOT ({target_column}::text ~ '{comparator}')
                        THEN true
                        ELSE false
                        END"""

        return self._do_check_operator(f"{target_column}_not_matches_regex", sql)
