from .base_sql_operator import BaseSqlOperator


class SharesNoElementsWithOperator(BaseSqlOperator):
    """Operator for checking if values share no elements."""

    def execute_operator(self, other_value):
        """target: str = self.replace_prefix(other_value.get("target"))
        comparator: str = self.replace_prefix(other_value.get("comparator"))

        def check_no_shared_elements(row):
            target_set = set(row[target]) if isinstance(row[target], (list, set)) else {row[target]}
            comparator_set = set(row[comparator]) if isinstance(row[comparator], (list, set)) else {row[comparator]}
            return len(target_set.intersection(comparator_set)) == 0

        return self.validation_df.apply(check_no_shared_elements, axis=1).all()"""
        raise NotImplementedError("shares_no_elements_with check_operator not implemented")
