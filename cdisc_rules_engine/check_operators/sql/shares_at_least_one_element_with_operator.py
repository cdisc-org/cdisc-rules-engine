from .base_sql_operator import BaseSqlOperator


class SharesAtLeastOneElementWithOperator(BaseSqlOperator):
    """Operator for checking if values share at least one element."""

    def execute_operator(self, other_value):
        """target: str = self.replace_prefix(other_value.get("target"))
        comparator: str = self.replace_prefix(other_value.get("comparator"))

        def check_shared_elements(row):
            target_set = set(row[target]) if isinstance(row[target], (list, set)) else {row[target]}
            comparator_set = set(row[comparator]) if isinstance(row[comparator], (list, set)) else {row[comparator]}
            return bool(target_set.intersection(comparator_set))

        return self.validation_df.apply(check_shared_elements, axis=1).any()"""
        raise NotImplementedError("shares_at_least_one_element_with check_operator not implemented")
