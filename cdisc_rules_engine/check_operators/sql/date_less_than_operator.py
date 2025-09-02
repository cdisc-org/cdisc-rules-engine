from .base_sql_operator import BaseSqlOperator


class DateLessThanOperator(BaseSqlOperator):
    """Operator for date less-than comparisons."""

    def execute_operator(self, other_value):
        """Check if target date is less than comparator date"""
        return self._date_comparison(other_value, "<")
