from .base_sql_operator import BaseSqlOperator


class DateNotEqualToOperator(BaseSqlOperator):
    """Operator for date inequality comparisons."""

    def execute_operator(self, other_value):
        """Check if target date does not equal comparator date"""
        return self._date_comparison(other_value, "!=")
