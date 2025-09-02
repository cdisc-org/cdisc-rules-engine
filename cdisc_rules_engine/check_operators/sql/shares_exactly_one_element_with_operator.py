from .base_sql_operator import BaseSqlOperator


class SharesExactlyOneElementWithOperator(BaseSqlOperator):
    """Operator for checking if values share exactly one element."""

    def execute_operator(self, other_value):
        """target: str = self.replace_prefix(other_value.get("target"))
        comparator: str = self.replace_prefix(other_value.get("comparator"))

        def check_exactly_one_shared_element(row):
            target_set = set(row[target]) if isinstance(row[target], (list, set)) else {row[target]}
            comparator_set = set(row[comparator]) if isinstance(row[comparator], (list, set)) else {row[comparator]}
            return len(target_set.intersection(comparator_set)) == 1

        return self.validation_df.apply(check_exactly_one_shared_element, axis=1).any()"""
        raise NotImplementedError("shares_exactly_one_element_with check_operator not implemented")
