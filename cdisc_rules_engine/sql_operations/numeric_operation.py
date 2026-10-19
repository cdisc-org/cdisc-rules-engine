from typing import Optional
from cdisc_rules_engine.sql_operations.aggregate_operation import AggregateSource, SqlAggregateOperation

OPERATION_NAMES = {"MAX": "max", "MIN": "min", "AVG": "mean", "COUNT": "record_count"}


class SqlNumericOperation(SqlAggregateOperation):

    @property
    def operation_name(self) -> str:
        return OPERATION_NAMES.get(self.function, self.function.lower())

    def _dataset_value(self, table: str):
        # Special case for counting size of whole dataset
        if self.params.target is None:
            return "*", "Num"
        return self._dataset_column(table).hash, "Num"

    def _can_aggregate(self, subtype: Optional[str]) -> bool:
        return self.function == "COUNT" or subtype == "Num"

    def _aggregate_sql(self, value_sql: str, aggregate_filter: str = "") -> str:
        return f"{self.function}({value_sql}){aggregate_filter}"

    def _group_value_sql(self) -> str:
        return "COALESCE({value}, 0)" if self.function == "COUNT" else "{value}"

    def _result_subtype(self, source: AggregateSource) -> str:
        return "Num"
