from abc import abstractmethod
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.exceptions.custom_exceptions import ColumnNotFoundError, RuleExecutionError
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation

PREVIOUS_OPERATION_ALIAS = "inner_op"


@dataclass
class AggregateSource:
    from_sql: str
    value_sql: Optional[str]
    subtype: Optional[str]
    resolve_column: Callable[[str], Optional[Tuple[str, str]]]
    previous_operation_name: Optional[str] = None


class SqlAggregateOperation(SqlBaseOperation):
    """
    Base for operations that aggregate values into one.
    This includes max, min, record_count, mean, max_date and min_date.
    """

    def __init__(self, params: SqlOperationParams, data_service: PostgresQLDataService, function: str):
        super().__init__(params, data_service)
        self.function = function

    @property
    @abstractmethod
    def operation_name(self) -> str:
        """Operation name as written in rules."""

    @abstractmethod
    def _dataset_value(self, table: str) -> Tuple[Optional[str], Optional[str]]:
        """The SQL for the values to aggregate from the dataset and their subtype."""

    @abstractmethod
    def _can_aggregate(self, subtype: Optional[str]) -> bool:
        """Whether this operation can aggregate over a previous operation's values of this subtype."""

    @abstractmethod
    def _aggregate_sql(self, value_sql: str, aggregate_filter: str = "") -> str:
        """The aggregate expression selected as the operation's value."""

    def _group_aggregate_sql(self, value_sql: str, aggregate_filter: str) -> str:
        """The aggregate expression selected as each group's value in group_by_query."""
        return self._aggregate_sql(value_sql, aggregate_filter)

    def _group_value_sql(self) -> str:
        """
        How a group's value from group_by_query reads as the answer `query` gives for a row of that group,
        with {value} standing for the group's value. Differs from the value itself where the aggregate answers
        something other than NULL when there is nothing to aggregate.
        """
        return "{value}"

    def _result_subtype(self, source: AggregateSource) -> Optional[str]:
        return source.subtype

    def _execute_operation(self):
        source = self._source()
        conditions = self._conditions(source)
        subtype = self._result_subtype(source)

        if not self.params.grouping:
            query = f"SELECT {self._aggregate_sql(source.value_sql)} AS value FROM {source.from_sql}"
            return SqlOperationResult(query=self._with_where(query, conditions), type="constant", subtype=subtype)

        grouping = [self._resolve_column(source, name, "group") for name in self.params.grouping]
        group_by_query, group_by_columns = self._group_by_query(source, conditions, grouping)

        params = {}
        row_conditions = list(conditions)
        for i, (name, column_sql) in enumerate(grouping):
            param_name = f"${i + 1}"
            row_conditions.append(f"({column_sql} = {param_name} OR ({column_sql} IS NULL AND {param_name} IS NULL))")
            params[param_name] = name

        query = f"SELECT {self._aggregate_sql(source.value_sql)} AS value FROM {source.from_sql}"
        return SqlOperationResult(
            query=self._with_where(query, row_conditions),
            type="constant",
            subtype=subtype,
            params=params,
            group_by_query=group_by_query,
            group_by_columns=group_by_columns,
            group_value_sql=self._group_value_sql(),
        )

    def _source(self) -> AggregateSource:
        previous_operation = (self.params.previous_operations or {}).get(self.params.target)
        if previous_operation is not None:
            return self._previous_operation_source(previous_operation)

        table = self._table()
        schema = self.data_service.pgi.schema

        def resolve_column(name: str) -> Optional[Tuple[str, str]]:
            column = schema.get_column(table, name)
            return (column.name, column.hash) if column else None

        value_sql, subtype = self._dataset_value(table)
        return AggregateSource(
            from_sql=schema.get_table_hash(table),
            value_sql=value_sql,
            subtype=subtype,
            resolve_column=resolve_column,
        )

    def _table(self) -> str:
        if self.params.use_rule_type_table:
            return self.params.table
        dataset_metadata = self.params.dataset_metadata
        if (
            dataset_metadata is not None
            and self.params.domain == dataset_metadata.domain
            and self.data_service.pgi.schema.get_table(self.params.domain) is None
        ):
            return dataset_metadata.name
        return self.params.domain

    def _dataset_column(self, table: str) -> SqlColumnSchema:
        column = self.data_service.pgi.schema.get_column(table, self.params.target)
        if column is None:
            raise ColumnNotFoundError(column_name=self.params.target, table_id=table)
        return column

    def _previous_operation_source(self, previous_operation: SqlOperationResult) -> AggregateSource:
        if previous_operation.group_by_query is None:
            raise RuleExecutionError(
                f"Operation {self.operation_name} can only reference a previous operation that "
                f"was itself grouped, but {self.params.target} has no grouped result to "
                f"aggregate over."
            )
        if not self._can_aggregate(previous_operation.subtype):
            raise RuleExecutionError(
                f"Operation {self.operation_name} cannot aggregate over the values of "
                f"{self.params.target} ({previous_operation.subtype})."
            )

        group_by_columns = previous_operation.group_by_columns or {}

        def resolve_column(name: str) -> Optional[Tuple[str, str]]:
            alias = group_by_columns.get(name.lower())
            return (name.lower(), f"{PREVIOUS_OPERATION_ALIAS}.{alias}") if alias else None

        return AggregateSource(
            from_sql=f"({previous_operation.group_by_query}) AS {PREVIOUS_OPERATION_ALIAS}",
            value_sql=f"{PREVIOUS_OPERATION_ALIAS}.value",
            subtype=previous_operation.subtype,
            resolve_column=resolve_column,
            previous_operation_name=self.params.target,
        )

    def _resolve_column(self, source: AggregateSource, name: str, usage: str) -> Tuple[str, str]:
        column = source.resolve_column(name)
        if column is not None:
            return column
        if source.previous_operation_name is not None:
            raise RuleExecutionError(
                f"Operation {self.operation_name} can only {usage} by the grouping columns of "
                f"{source.previous_operation_name}, but {name} is not one of them."
            )
        raise ColumnNotFoundError(column_name=name, table_id=self._table())

    def _conditions(self, source: AggregateSource) -> List[str]:
        conditions = []
        filter_clause = self.construct_where_clause(
            resolve_column=lambda name: self._resolve_column(source, name, "filter")[1]
        )
        if filter_clause:
            conditions.append(filter_clause.removeprefix("WHERE "))
        if self.params.regex and source.value_sql != "*":
            pattern = self.params.regex.replace("'", "''")
            conditions.append(f"{source.value_sql}::text ~ '{pattern}'")
        return conditions

    def _group_by_query(
        self, source: AggregateSource, conditions: List[str], grouping: List[Tuple[str, str]]
    ) -> Tuple[str, dict]:
        aggregate_filter, group_where = "", []
        if conditions:
            if self.params.ignore_empty_filtered_groups:
                group_where = conditions
            else:
                aggregate_filter = f" FILTER (WHERE {' AND '.join(conditions)})"

        group_by_columns = {name: f"group_{i + 1}" for i, (name, _) in enumerate(grouping)}
        selected_columns = "".join(f", {column_sql} AS {group_by_columns[name]}" for name, column_sql in grouping)
        query = (
            f"SELECT {self._group_aggregate_sql(source.value_sql, aggregate_filter)} AS value{selected_columns} "
            f"FROM {source.from_sql}"
        )
        query = self._with_where(query, group_where)
        query += f" GROUP BY {', '.join(column_sql for _, column_sql in grouping)}"
        return query, group_by_columns

    @staticmethod
    def _with_where(query: str, conditions: List[str]) -> str:
        return f"{query} WHERE {' AND '.join(conditions)}" if conditions else query
