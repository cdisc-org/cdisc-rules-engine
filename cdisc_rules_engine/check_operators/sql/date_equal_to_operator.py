from .base_sql_operator import BaseSqlOperator


class DateEqualToOperator(BaseSqlOperator):
    """Operator for date equality comparisons."""

    def execute_operator(self, other_value):
        """Check if target date equals comparator date"""
        return self._date_comparison(other_value, "=")
