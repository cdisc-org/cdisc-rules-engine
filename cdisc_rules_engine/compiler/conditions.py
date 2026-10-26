"""
The all/any tree of a rule's checks -> one SQL boolean over the source rows.

Operators can't always express themselves as a plain predicate.
Two escape hatches:
  - window columns - anything needing to see neighbouring rows (LEAD, ROW_NUMBER, ...).
    PostgreSQL won't allow window functions in WHERE (sad face) SO we put these into a subquery around the
    source and the predicate just reads the column.
  - group joins - pre-aggregated relations LEFT JOIN'd onto the rows (uniqueness checks etc.)
"""

from dataclasses import dataclass, field
from typing import Dict, Optional

from cdisc_rules_engine.check_operators.sql import PostgresQLOperators
from cdisc_rules_engine.compiler.derived import SOURCE_ALIAS, DerivedColumnRegistry, GroupJoin
from cdisc_rules_engine.exceptions.custom_exceptions import RuleFormatError
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema

CO = "co"
GROUP_KEYS = ("all", "any", "not")


@dataclass
class CompiledConditions:
    where: str
    derived_columns: DerivedColumnRegistry
    joins: Dict[str, GroupJoin] = field(default_factory=dict)

    def from_clause(self, table_hash: str) -> str:
        if self.derived_columns:
            select = ", ".join([f"{SOURCE_ALIAS}.*", *self.derived_columns.select_items()])
            source = f"(SELECT {select} FROM {table_hash} {SOURCE_ALIAS}) {CO}"
        else:
            source = f"{table_hash} {CO}"
        joins = "".join(f"\nLEFT JOIN ({join.query}) AS {join.alias} ON {join.on}" for join in self.joins.values())
        return f"{source}{joins}"


class ConditionCompiler:
    """
    One per rule.
    Not reusable across rules because derived columns and joins accumulate on the instance
    as operators register them.
    """

    def __init__(
        self,
        data_service,
        dataset_id: str,
        dataset_metadata,
        operation_variables: Optional[dict] = None,
        extra_operator_data: Optional[dict] = None,
    ):
        self.data_service = data_service
        self.dataset_id = dataset_id
        self.dataset_metadata = dataset_metadata
        self.operation_variables = operation_variables or {}
        self.extra_operator_data = extra_operator_data or {}
        self.derived_columns = DerivedColumnRegistry(_schema_copy(data_service.pgi.schema.get_table(dataset_id)), CO)
        self.joins: Dict[str, GroupJoin] = {}

    def compile(self, conditions) -> CompiledConditions:
        if hasattr(conditions, "to_dict"):
            conditions = conditions.to_dict()
        where = self._compile_node(conditions) if conditions else "FALSE"
        return CompiledConditions(where, self.derived_columns, self.joins)

    def compile_condition(self, operator: str, value: dict) -> CompiledConditions:
        """A single check as a rule with one condition."""
        return CompiledConditions(self._evaluate(operator, value), self.derived_columns, self.joins)

    def _compile_node(self, node) -> str:
        if hasattr(node, "to_dict"):
            node = node.to_dict()
        groups = [key for key in GROUP_KEYS if key in node]
        if len(groups) > 1 or (groups and "operator" in node):
            raise RuleFormatError(
                f"A condition must be exactly one of {', '.join(GROUP_KEYS)} or a check, but has: "
                f"{', '.join(groups + (['operator'] if 'operator' in node else []))}"
            )
        for key, joiner in (("all", " AND "), ("any", " OR ")):
            if key in node:
                if not node[key]:
                    raise RuleFormatError(f"A condition's '{key}' must hold at least one check")
                return f"({joiner.join(self._compile_node(child) for child in node[key])})"
        if "not" in node:
            # almost equivalent to NotOperator but without COALESCE
            return f"(NOT {self._compile_node(node['not'])})"
        return self._evaluate(node.get("operator"), node.get("value", {}))

    def _evaluate(self, operator: str, value: dict) -> str:
        # a fresh PostgresQLOperators per leaf is cheap so I'm not worried about instantiating it repeatedly.
        return PostgresQLOperators(self._operator_data()).evaluate(operator, value)

    def _operator_data(self) -> dict:
        return {
            "column_prefix_map": {"--": self.dataset_metadata.domain if self.dataset_metadata else None},
            **self.extra_operator_data,
            "dataset_id": self.dataset_id,
            "data_service": self.data_service,
            "dataset_metadata": self.dataset_metadata,
            "operation_variables": self.operation_variables,
            "derived_columns": self.derived_columns,
            "join_scope": self.joins,
        }


def _schema_copy(schema: SqlTableSchema) -> SqlTableSchema:
    # window columns get registered on this copy
    # the real table's schema never sees them
    if schema is None:
        raise ValueError("The rule's source table has no schema")
    copy = SqlTableSchema(schema.name, schema.hash, schema.source)
    for _, column in schema.get_columns():
        copy.add_column(column)
    return copy
