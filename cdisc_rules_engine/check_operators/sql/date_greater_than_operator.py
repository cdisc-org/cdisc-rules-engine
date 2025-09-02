from .base_sql_operator import BaseSqlOperator


class DateGreaterThanOperator(BaseSqlOperator):
    """Operator for date greater-than comparisons."""

    def execute_operator(self, other_value):
        """Check if target date is greater than comparator date"""
        return self._date_comparison(other_value, ">")
