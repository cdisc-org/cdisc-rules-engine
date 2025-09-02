from .suffix_matches_regex_operator import SuffixMatchesRegexOperator


class NotSuffixMatchesRegexOperator(SuffixMatchesRegexOperator):
    """Operator for inverted suffix regex pattern matching."""

    def execute_operator(self, other_value):
        """target = self.replace_prefix(other_value.get("target"))
        comparator = other_value.get("comparator")
        suffix = other_value.get("suffix")
        converted_strings = self.validation_df[target].map(lambda x: self._custom_str_conversion(x))
        results = converted_strings.notna() & ~converted_strings.astype(str).map(
            lambda x: re.search(comparator, x[-suffix:]) is not None
        )
        return results"""
        raise NotImplementedError("not_suffix_matches_regex check_operator not implemented")
