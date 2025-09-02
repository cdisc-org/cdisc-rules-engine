from .base_sql_operator import BaseSqlOperator


class IsOrderedByOperator(BaseSqlOperator):
    """Operator for checking if data is ordered by specified columns."""

    def execute_operator(self, other_value):
        """
        Checking validity based on target order.
        """
        """target: str = self.replace_prefix(other_value.get("target"))
        sort_order: str = other_value.get("order", "asc")
        if sort_order not in ["asc", "dsc"]:
            raise ValueError("invalid sorting order")
        sort_order_bool: bool = sort_order == "asc"
        return (
            self.validation_df[target]
            .eq(self.validation_df[target].sort_values(ascending=sort_order_bool, ignore_index=True))
            .astype(bool)
        )"""
        raise NotImplementedError("is_ordered_by check_operator not implemented")
