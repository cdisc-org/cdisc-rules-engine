from .base_sql_operator import BaseSqlOperator


class ReferencesCorrectCodelistOperator(BaseSqlOperator):
    """Operator for checking if value references correct codelist."""

    def execute_operator(self, other_value):
        """target: str = self.replace_prefix(other_value.get("target"))
        comparator = self.replace_prefix(other_value.get("comparator"))
        result = self.validation_df.apply(
            lambda row: self.valid_codelist_reference(row[target], row[comparator]),
            axis=1,
        )
        return result"""
        raise NotImplementedError("references_correct_codelist check_operator not implemented")
