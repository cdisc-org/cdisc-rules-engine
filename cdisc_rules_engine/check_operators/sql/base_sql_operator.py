import hashlib
import re
from abc import abstractmethod
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd

from cdisc_rules_engine.compiler.derived import SOURCE_ALIAS, GroupJoin
from cdisc_rules_engine.constants.metadata_columns import DATASET_NAME, SOURCE_DS
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.data_service.util import safe_numeric_cast_sql
from cdisc_rules_engine.exceptions.custom_exceptions import ColumnNotFoundError
from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata

CHECK_OPERATOR_TABLE_ALIAS = "co"


class BaseSqlOperator:
    """Base class for individual SQL-based check operators (similar to BaseOperation)."""

    def __init__(self, data):
        self.original_data = data
        self.validation_df: DatasetInterface = data.get("df", PandasDataset(data=pd.DataFrame()))
        self.table_id: str = data["dataset_id"]
        self.sql_data_service: PostgresQLDataService = data["data_service"]
        self.column_prefix_map = data.get("column_prefix_map", {})
        self.value_level_metadata = data.get("value_level_metadata", [])
        self.column_codelist_map = data.get("column_codelist_map", {})
        self.codelist_term_maps = data.get("codelist_term_maps", [])
        self.operation_variables: dict[str, SqlOperationResult] = data.get("operation_variables", {})
        self.dataset_metadata: BaseDatasetMetadata = data.get("dataset_metadata", None)
        self.derived_columns = data.get("derived_columns")

    @abstractmethod
    def execute_operator(self, other_value: Dict[str, Any]):
        """
        Execute the operator logic.
        If the required columns are missing, it will return a default sql result specific to the operator.
        """
        pass

    def has_missing_required_columns(self, other_value: Dict[str, Any]) -> bool:
        """
        Whether execute_operator would fall back to a default result (e.g. FALSE) because a
        required column is missing, rather than a real computed result.

        NotOperator uses this to decide whether to negate: negating a "couldn't check, so
        FALSE" default would silently turn it into a false positive/negative depending on
        polarity, so operators with such defaults should override this to report when they
        apply, and NotOperator leaves the default result untouched when they do.
        """
        return False

    def _assert_valid_value_and_cast(self, value):
        return value

    def _custom_str_conversion(self, x):
        if pd.notna(x):
            if isinstance(x, int):
                return str(x).strip()
            elif isinstance(x, float):
                return f"{x:.0f}" if x.is_integer() else str(x).strip()
        return x

    def convert_string_data_to_lower(self, data):
        if self.validation_df.is_series(data):
            data = data.str.lower()
        else:
            data = data.lower()
        return data

    def replace_prefix(self, value: str) -> Union[str, Any]:
        if isinstance(value, str):
            for prefix, replacement in self.column_prefix_map.items():
                if value.startswith(prefix) and replacement is not None:
                    return value.replace(prefix, replacement, 1)
        return value

    def replace_all_prefixes(self, values: List[str]) -> List[str]:
        for i in range(len(values)):
            values[i] = self.replace_prefix(values[i])
        return values

    def get_comparator_data(self, comparator, value_is_literal: bool = False):
        if value_is_literal:
            return comparator
        else:
            return self.validation_df.get(comparator, comparator)

    def is_column_of_iterables(self, column):
        """return self.validation_df.is_series(column) and (
            isinstance(column.iloc[0], list) or isinstance(column.iloc[0], set)
        )"""
        raise NotImplementedError("is_column_of_iterables check_operator not implemented")

    def _exists(self, column: str) -> bool:
        return self.sql_data_service.pgi.schema.column_exists(self.table_id, column)

    def _do_check_operator(self, sql_subquery_fn) -> str:
        """Operator condition expressed as a sql bool over the rule's source rows (aliased co)."""
        return sql_subquery_fn()

    def _source_column_sql(self, column: str) -> str:
        """Column as addressed inside the source subquery (aliased t), where window expressions are computed."""
        return f"{SOURCE_ALIAS}.{self._column_sql(column, alias=False)}"

    def _window_column(self, name: str, expression: str, column_type: str = "Bool") -> str:
        """
        Register an expression that has to see across rows (a window function) on the source
        subquery, and return the condition reading it. No column is ever added to the table.
        """
        if self.derived_columns is None:
            raise ValueError(f"'{name}' needs the rule's source subquery to compute across rows, but none was given")
        name = self._resolve_operation_variables_in_cache_key(name)
        return self.derived_columns.add(name, expression, column_type)

    def _get_cache_key_component(self, value) -> str:
        """
        Get a cache-safe component for column naming.
        If value is an operation variable, hash its resolved query.
        Otherwise, return the value as a string.

        This ensures that different operation results (e.g., SELECT TRUE vs SELECT FALSE)
        get different cache keys, preventing incorrect cache reuse across datasets.
        """
        if isinstance(value, str) and value in self.operation_variables:
            # Get the resolved query from the operation variable
            op_var = self.operation_variables[value]
            resolved_query = op_var.query

            # Hash the query to create a unique, fixed-length identifier
            query_hash = hashlib.md5(resolved_query.encode()).hexdigest()[:8]
            return f"op_{query_hash}"

        return str(value)

    def _resolve_operation_variables_in_cache_key(self, cache_key: str) -> str:
        """
        Replace operation variable names ($variable) in cache key with their query hashes.
        """
        result = cache_key
        for var_name in self.operation_variables:
            if var_name in result:
                var_hash = self._get_cache_key_component(var_name)
                result = result.replace(var_name, var_hash)
        return result

    def _table_sql(self):
        return self.sql_data_service.pgi.schema.get_table_hash(self.table_id)

    def _column_sql(
        self,
        column: str,
        lowercase: bool = False,
        prefix: Optional[int] = None,
        suffix: Optional[int] = None,
        alias: bool = True,
        null_return: bool = False,
    ) -> str:
        if column == DATASET_NAME:
            if getattr(self.dataset_metadata, "split_part_filenames", None) and self._exists(SOURCE_DS):
                return self._column_sql(SOURCE_DS, lowercase=lowercase, prefix=prefix, suffix=suffix, alias=alias)
            dataset_name = self.dataset_metadata.name
            if prefix is not None:
                dataset_name = dataset_name[: int(prefix)]
            elif suffix is not None:
                dataset_name = dataset_name[-int(suffix) :] if int(suffix) > 0 else ""
            return self._constant_sql(dataset_name, lowercase=lowercase)

        if not self._exists(column):
            if null_return:
                return "NULL"
            raise ColumnNotFoundError(
                column_name=column,
                table_id=self.table_id,
                message=f"Column '{column}' not found in table '{self.table_id}'",
            )

        query = self.sql_data_service.pgi.schema.get_column_hash(self.table_id, column)
        # Prepend the table alias
        query = f"{CHECK_OPERATOR_TABLE_ALIAS}.{query}" if alias else query

        if lowercase:
            query = f"LOWER({query})"
        if prefix is not None:
            query = f"LEFT({query}, {prefix})"
        if suffix is not None:
            query = f"RIGHT({query}, {suffix})"
        return query

    def _constant_sql(self, value: Any, lowercase: bool = False) -> str:
        """
        Generates a SQL constant value based on the type of the value.
        Will resolve any operation variables it finds.
        Will map None to an empty string.
        """
        if isinstance(value, str):
            return self._handle_string_constant(value, lowercase)
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, (int, float)):
            return str(value)
        elif value is None:
            return "NULL"
        else:
            raise ValueError(f"Unsupported constant type: {type(value)}")

    def _handle_string_constant(self, value: str, lowercase: bool) -> str:
        """Handle string constants, including operation variables."""
        if value in self.operation_variables:
            query = self._process_constant_operation_variable(value)
        else:
            query = f"'{value.replace("'", "''")}'"

        if lowercase:
            query = f"LOWER({query})"
        return query

    def _process_constant_operation_variable(self, value: str) -> str:
        """Process a constant operation variable."""
        variable = self.operation_variables[value]
        if variable.type != "constant":
            raise ValueError(f"Variable {value} is not a constant.")

        query = variable.query
        if variable.params:
            joined_value = self._join_group_values(variable)
            if joined_value is not None:
                return joined_value
            query = self._substitute_operation_parameters(query, variable.params)
        return f"({query})"

    @property
    def _join_scope(self) -> Optional[Dict[str, GroupJoin]]:
        return self.original_data.get("join_scope")

    def _join_derived(self, query: str, on: Callable[[str], str]) -> str:
        """Join a pre-aggregated relation to the rows being checked, and return its alias."""
        if self._join_scope is None:
            raise ValueError("A cross-row check needs the compiled rule's join scope, but none was given")
        # Alias is derived from the SQL, so the same join requested twice (e.g. the check appears
        # in two branches of an any) collapses into one entry in the join scope
        alias = "drv_" + hashlib.md5(f"{query}{on('_')}".encode()).hexdigest()[:10]
        self._join_scope[alias] = GroupJoin(alias, query, on(alias))
        return alias

    def _join_group_values(self, variable: SqlOperationResult) -> Optional[str]:
        """
        For a grouped operation, join its value for every group to the rows being checked, and use the joined
        value, instead of asking each row for its group's value with a subquery that scans the whole table again.
        Returns None where the operation can't be joined, so it is asked for row by row.
        """
        grouping = variable.group_by_columns
        if self._join_scope is None or variable.group_by_query is None:
            return None
        if not grouping or len(variable.params) != len(grouping):
            return None
        if not all(re.fullmatch(r"\$\d+", placeholder) for placeholder in variable.params):
            return None

        identity = f"{variable.group_by_query}{sorted(variable.params.items())}"
        alias = "grp_" + hashlib.md5(identity.encode()).hexdigest()[:10]
        conditions = []
        for placeholder, column_name in variable.params.items():
            group_column = f"{alias}.group_{int(placeholder[1:])}"
            row_value = self._column_sql(column_name, null_return=True)
            conditions.append(
                f"({group_column} IS NULL) = ({row_value} IS NULL) "
                f"AND COALESCE(CAST({group_column} AS TEXT), '') = COALESCE(CAST({row_value} AS TEXT), '')"
            )
        self._join_scope[alias] = GroupJoin(alias, variable.group_by_query, " AND ".join(conditions))
        return variable.group_value_sql.format(value=f"{alias}.value")

    def _substitute_operation_parameters(self, query: str, params: dict) -> str:
        """
        Substitute operation parameters in query.
        """
        for param_placeholder, column_name in params.items():
            column_sql = self._column_sql(column_name, null_return=True)
            query = query.replace(param_placeholder, column_sql)
        return query

    def _collection_sql(self, value: Any, lowercase: bool = False) -> str:
        """
        Generates a SQL collection string based on the type of the value.
        """
        if isinstance(value, list):
            return f"({', '.join(self._constant_sql(v, lowercase=lowercase) for v in value)})"
        elif isinstance(value, str):
            return self._handle_string_collection(value, lowercase)
        elif value is None:
            return ""
        else:
            raise ValueError(f"Unsupported collection type: {type(value)}")

    def _handle_string_collection(self, value: str, lowercase: bool) -> str:
        """Handle string collections (operation variables)."""
        if value not in self.operation_variables:
            raise ValueError(f"Expected a collection, got a string: {value}")

        variable = self.operation_variables[value]
        if variable.type != "collection":
            raise ValueError(f"Variable {value} is not a collection.")

        query = self._process_collection_operation_variable(variable, lowercase)
        return query

    def _process_collection_operation_variable(self, variable, lowercase: bool) -> str:
        """Process a collection operation variable."""
        query = variable.query

        if variable.params:
            query = self._substitute_operation_parameters(query, variable.params)

        query = f"({query})"
        if lowercase:
            query = self._apply_lowercase_to_collection(query)
        return query

    def _apply_lowercase_to_collection(self, query: str) -> str:
        """Apply lowercase to collection query results."""
        return f"(SELECT LOWER(value) FROM {query})"

    def _sql(self, value: Any, lowercase: bool = False, value_is_literal: bool = False) -> str:
        """
        Tries to generate a general SQL query. Will resolve a column is possible
        or an operation variable if possible otherwise assumes it's a constant
        """
        if isinstance(value, str) and not value_is_literal:
            if value in self.operation_variables:
                variable = self.operation_variables[value]
                if variable.type == "constant":
                    return self._constant_sql(value, lowercase=lowercase)
                elif variable.type == "collection":
                    return self._collection_sql(value, lowercase=lowercase)
                else:
                    raise ValueError(f"Unsupported variable type: {variable.type} for variable {value}.")
            elif self.sql_data_service.pgi.schema.column_exists(self.table_id, value):
                return self._column_sql(value, lowercase=lowercase)

        return self._constant_sql(value, lowercase=lowercase)

    def _is_numeric_value(self, value: Any, value_is_literal: bool = False) -> bool:
        """
        Check if a value represents a numeric type that can be used directly in numeric comparisons.
        """
        # Direct numeric literals
        if isinstance(value, (int, float)):
            return True

        # Numeric string literals
        if value_is_literal and isinstance(value, str) and value.isdigit():
            return True

        if not value_is_literal and isinstance(value, str):
            # Check operation variables
            if value in self.operation_variables:
                variable = self.operation_variables[value]
                return variable.type == "constant" and variable.subtype == "Num"

            # Check column types
            if self.sql_data_service.pgi.schema.column_exists(self.table_id, value):
                value_column = self.replace_prefix(value).lower()
                col_schema = self.sql_data_service.pgi.schema.get_column(self.table_id, value_column)
                return col_schema and col_schema.type == "Num"

        return False

    def valid_codelist_reference(self, column_name, codelist):
        if column_name in self.column_codelist_map:
            return codelist in self.column_codelist_map[column_name]
        elif self.column_prefix_map:
            # Check for generic versions of variables (i.e --DECOD)
            for key in self.column_prefix_map:
                if column_name.startswith(self.column_prefix_map[key]):
                    generic_column_name = column_name.replace(self.column_prefix_map[key], key, 1)
                    if generic_column_name in self.column_codelist_map:
                        return codelist in self.column_codelist_map.get(generic_column_name)
        return True

    def _get_string_part_series(self, part_to_validate: str, length: int, target: str):
        """if not self.validation_df[target].apply(type).eq(str).all():
            raise ValueError("The operator can't be used with non-string values")

        if part_to_validate == "suffix":
            series_to_validate = self.validation_df[target].str.slice(-length)
        elif part_to_validate == "prefix":
            series_to_validate = self.validation_df[target].str.slice(stop=length)
        else:
            raise ValueError(
                f"Invalid part to validate: {part_to_validate}. \
                    Valid values are: suffix, prefix"
            )
        series_to_validate = series_to_validate.mask(pd.isna(self.validation_df[target]))
        return series_to_validate"""
        raise NotImplementedError("_get_string_part_series check_operator not implemented")

    def _check_equality_of_string_part(
        self,
        target: str,
        comparison_data,
        part_to_validate: str,
        length: int,
    ):
        """
        Checks if the given string part is equal to comparison data.
        """
        """series_to_validate = self._get_string_part_series(part_to_validate, length, target)
        return series_to_validate.eq(comparison_data).astype(bool)"""
        raise NotImplementedError("_check_equality_of_string_part check_operator not implemented")

    def _is_empty_sql(self, target: str, alias: bool = True) -> str:
        """
        Generates a SQL query to check target is empty, checks if it is an
        operation variable or column, otherwise assumes it's a constant.
        """
        if isinstance(target, str):
            if self.sql_data_service.pgi.schema.get_column(self.table_id, target) is not None:
                return self._is_empty_sql_column(target, alias)
            elif target in self.operation_variables:
                return self._is_empty_sql_operation_variable(target)

        if isinstance(target, str) and target == "":
            return "TRUE"
        elif target is None:
            return "TRUE"
        else:
            return "FALSE"

    def _is_empty_sql_column(self, col: str, alias: bool = True) -> str:
        """
        Generates a SQL query to check if a column is empty.
        """
        column = self.sql_data_service.pgi.schema.get_column(self.table_id, col)
        if not column:
            raise ColumnNotFoundError(col, self.table_id)

        key = column.hash
        if alias:
            key = f"{CHECK_OPERATOR_TABLE_ALIAS}.{key}"

        match column.type:
            case "Char":
                return f"({key} IS NULL OR {key} = '')"
            case "Bool":
                return f"({key} IS NULL)"
            case "Num":
                return f"({key} IS NULL)"
            case "Date":
                return f"({key} IS NULL)"
            case _:
                raise ValueError(f"Unsupported column type: {column.type} for column {col}.")

    def _is_empty_sql_operation_variable(self, target: str) -> str:
        """
        Generates a SQL query to check if an operation variable is empty.
        """
        variable = self.operation_variables[target]
        if not variable:
            raise ValueError(f"Variable {target} does not exist.")

        if variable.type != "constant":
            query = variable.query
            if variable.params:
                query = self._substitute_operation_parameters(query, variable.params)
            return f"(NOT EXISTS (SELECT 1 FROM ({query}) AS op))"

        value = self._process_constant_operation_variable(target)

        # Check if the resolved query result is empty based on variable subtype
        match variable.subtype:
            case "Char":
                return f"({value} IS NULL OR {value} = '')"
            case "Bool":
                return f"({value} IS NULL)"
            case "Num":
                return f"({value} IS NULL)"
            case "Date":
                return f"({value} IS NULL)"
            case _:
                raise ValueError(f"Unsupported variable type: {variable.subtype} for variable {target}.")

    def _filter_params(self, other_value, ex_dict_table_name):
        filter_attribute = other_value.get("filter_attribute")
        filter_value = other_value.get("filter_value")

        if filter_attribute and filter_value:
            if not self.sql_data_service.pgi.schema.column_exists(ex_dict_table_name, filter_attribute):
                raise ValueError(f"Filter attribute '{filter_attribute}' is not a column in {ex_dict_table_name}.")

            if filter_value in self.operation_variables:
                attribute_op_result = self.operation_variables[filter_value]
                if attribute_op_result.type != "constant":
                    raise ValueError(
                        f"Filter value operation '{filter_value}' must be a constant result "
                        f"to be used as a filter value."
                    )
                self.sql_data_service.pgi.execute_sql(attribute_op_result.query)
                filter_value = self.sql_data_service.pgi.fetch_one()["value"]

            filter_value = filter_value.replace("'", "").replace('"', "").strip()

        return filter_attribute, filter_value

    @staticmethod
    def _safe_numeric_cast_sql(value_sql: str) -> str:
        """
        Safely cast a SQL expression to NUMERIC.
        Non-numeric values return NULL instead of raising a Postgres cast error.
        """
        return safe_numeric_cast_sql(value_sql)

    @staticmethod
    def _version_le_condition_sql(column: str, filter_value: str) -> str:
        """
        SQL condition equivalent to "{column} <= '{filter_value}'" that compares
        dotted numeric versions.
        Falls back to string comparison if either are not dotted numeric versions.
        """
        dotted_numeric_pattern = r"^\d+(\.\d+)*$"
        return f"""(
            CASE
                WHEN {column} ~ '{dotted_numeric_pattern}' AND '{filter_value}' ~ '{dotted_numeric_pattern}'
                    THEN string_to_array({column}, '.')::int[] <= string_to_array('{filter_value}', '.')::int[]
                ELSE {column} <= '{filter_value}'
            END
        )"""
