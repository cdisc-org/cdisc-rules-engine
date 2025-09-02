from .base_sql_operator import BaseSqlOperator


class EqualsStringPartOperator(BaseSqlOperator):
    """Operator for checking if string part equals comparator."""

    def execute_operator(self, other_value):
        """
        Checks that the values in the target column
        equal the result of parsing the value in the comparison
        column with a regex
        """
        """target = self.replace_prefix(other_value.get("target"))
        comparator = other_value.get("comparator")
        regex = other_value.get("regex")
        value_is_literal: bool = other_value.get("value_is_literal", False)
        comparison_data = self.get_comparator_data(comparator, value_is_literal)
        if isinstance(comparison_data, str):
            parsed_data = apply_regex(regex, comparison_data)
        else:
            parsed_data = comparison_data.str.findall(regex).str[0]
        parsed_id = str(uuid4())
        self.validation_df[parsed_id] = parsed_data
        return self.validation_df.apply(
            lambda row: self._check_equality(row, target, parsed_id, value_is_literal),
            axis=1,
        )"""
        raise NotImplementedError("equals_string_part check_operator not implemented")
