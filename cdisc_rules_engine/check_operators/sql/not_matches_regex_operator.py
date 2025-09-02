from .matches_regex_operator import MatchesRegexOperator


class NotMatchesRegexOperator(MatchesRegexOperator):
    """Operator for inverted regex pattern matching."""

    def execute_operator(self, other_value):
        """target = self.replace_prefix(other_value.get("target"))
        comparator = other_value.get("comparator")
        converted_strings = self.validation_df[target].map(lambda x: self._custom_str_conversion(x))
        results = converted_strings.notna() & ~converted_strings.astype(str).str.match(comparator)
        return results"""
        raise NotImplementedError("not_matches_regex check_operator not implemented")
