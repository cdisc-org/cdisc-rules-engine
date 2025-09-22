from .base_sql_operator import BaseSqlOperator


class SuffixMatchesRegexOperator(BaseSqlOperator):
    """Operator for suffix regex pattern matching."""

    def __init__(self, data, invert=False):
        super().__init__(data)
        self.invert = invert

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target")).lower()
        target_column = self._column_sql(target)
        comparator = other_value.get("comparator")
        suffix = other_value.get("suffix")

        if self.invert:
            operator_name = f"{target_column}_not_suffix_matches_regex"
        else:
            operator_name = f"{target_column}_suffix_matches_regex"

        def sql():
            suffix_expr = f"RIGHT({target_column}::text, {suffix})"

            if self.invert:
                return f"""CASE
                        WHEN {self._is_empty_sql(target)} THEN FALSE
                        WHEN {suffix_expr} ~ '{comparator}' THEN FALSE
                        ELSE TRUE
                        END"""
            else:
                return f"""CASE
                        WHEN {self._is_empty_sql(target)} THEN FALSE
                        WHEN {suffix_expr} ~ '{comparator}' THEN TRUE
                        ELSE FALSE
                        END"""

        return self._do_check_operator(operator_name, sql)
