import traceback
from abc import abstractmethod
from functools import wraps
from typing import Any, List, Optional, Union

import numpy as np
import pandas as pd

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.services import logger


def log_operator_execution(func):
    @wraps(func)
    def wrapper(self, other_value, *args, **kwargs):
        try:
            logger.info(f"Starting check operator: {func.__name__}")
            result = func(self, other_value)
            logger.info(f"Completed check operator: {func.__name__}")
            return result
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}, " f"traceback: {traceback.format_exc()}")
            error_message = str(e)
            if isinstance(e, TypeError) and (
                "NoneType" in error_message
                or "None" in error_message
                or any(
                    phrase in error_message
                    for phrase in [
                        "NoneType",
                        "object is None",
                        "'NoneType'",
                        "None has no attribute",
                        "unsupported operand type",
                        "bad operand type",
                        "object is not",
                        "cannot be None",
                    ]
                )
            ):
                return None
            else:
                raise

    return wrapper


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

    @abstractmethod
    def execute_operator(self, other_value):
        """Execute the specific operator logic. Must be implemented by each operator."""
        pass

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

    @log_operator_execution
    def is_column_of_iterables(self, column):
        """return self.validation_df.is_series(column) and (
            isinstance(column.iloc[0], list) or isinstance(column.iloc[0], set)
        )"""
        raise NotImplementedError("is_column_of_iterables check_operator not implemented")

    def _exists(self, column: str) -> bool:
        return self.sql_data_service.pgi.schema.column_exists(self.table_id, column)

    def _fetch_for_venmo(self, column: str):
        """
        Fetches data from a SQL table and returns it as a pandas Series,
        so we can pass it to Venmo.
        """
        # Fetch all of the rows
        self.sql_data_service.pgi.execute_sql(
            f"SELECT id, {self._column_sql(column)} as data FROM {self._table_sql()};"
        )
        sql_results = self.sql_data_service.pgi.fetch_all()

        # Fix off-by-one
        return_series = pd.Series(data={item["id"] - 1: item["data"] for item in sql_results})
        return return_series

    def _do_check_operator(self, new_column: str, sql_subquery_fn):
        # Handles simple checks by creating a column and updating it with a scalar subquery.
        exists = self.sql_data_service.pgi.schema.column_exists(self.table_id, new_column)
        if not exists:
            self.sql_data_service.pgi.add_column(
                table=self.table_id, schema=SqlColumnSchema.generated(new_column, "Bool")
            )

            subquery = sql_subquery_fn()
            query = f"UPDATE {self._table_sql()} SET {self._column_sql(new_column)} = ({subquery});"
            self.sql_data_service.pgi.execute_sql(query)
        return self._fetch_for_venmo(new_column)

    def _do_complex_check_operator(self, new_column: str, sql_full_query_fn):
        # Handles complex checks by creating a column and populating it with a full custom query.
        exists = self.sql_data_service.pgi.schema.column_exists(self.table_id, new_column)
        if not exists:
            self.sql_data_service.pgi.add_column(
                table=self.table_id, schema=SqlColumnSchema.generated(new_column, "Bool")
            )
            query = sql_full_query_fn(self._table_sql(), self._column_sql(new_column))
            self.sql_data_service.pgi.execute_sql(query)
        return self._fetch_for_venmo(new_column)

    def _table_sql(self):
        return self.sql_data_service.pgi.schema.get_table_hash(self.table_id)

    def _column_sql(
        self, column: str, lowercase: bool = False, prefix: Optional[int] = None, suffix: Optional[int] = None
    ) -> str:
        query = self.sql_data_service.pgi.schema.get_column_hash(self.table_id, column)

        # TODO: Throwing this temporarily, so we can determine which errors
        # are actually postgres errors and which are just rules which run on
        # optional variables without checking
        if query is None:
            raise KeyError(column)

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
            if value in self.operation_variables:
                variable = self.operation_variables[value]
                if variable.type != "constant":
                    raise ValueError(f"Variable {value} is not a constant.")
                query = f"({variable.query})"
            else:
                query = f"'{value.replace("'", "''")}'"

            if lowercase:
                query = f"LOWER({query})"
            return query
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, (int, float)):
            return str(value)
        elif value is None:
            return "NULL"
        else:
            raise ValueError(f"Unsupported constant type: {type(value)}")

    def _collection_sql(self, value: Any, lowercase: bool = False) -> str:
        """
        Generates a SQL collection string based on the type of the value.
        """
        if isinstance(value, list):
            return f"({', '.join(self._constant_sql(v, lowercase=lowercase) for v in value)})"
        elif isinstance(value, str):
            if value in self.operation_variables:
                variable = self.operation_variables[value]
                if variable.type != "collection":
                    raise ValueError(f"Variable {value} is not a collection.")
                query = f"({variable.query})"
                if lowercase:
                    # column1 is the default column name
                    query = f"(SELECT LOWER(column1) FROM {query})"
                return query
            raise ValueError(f"Expected a collection, got a string: {value}")
        elif value is None:
            return ""
        else:
            raise ValueError(f"Unsupported collection type: {type(value)}")

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

    def _series_is_in(self, target, comparison_data):
        return np.where(comparison_data.isin(target), True, False)

    def _is_empty_sql(self, target: str) -> str:
        """
        Generates a SQL query to check target is empty, checks if it is an
        operation variable or column, otherwise assumes it's a constant.
        """
        if isinstance(target, str):
            if self.sql_data_service.pgi.schema.get_column(self.table_id, target) is not None:
                return self._is_empty_sql_column(target)
            elif target in self.operation_variables:
                return self._is_empty_sql_operation_variable(target)

        if isinstance(target, str) and target == "":
            return "TRUE"
        elif target is None:
            return "TRUE"
        else:
            return "FALSE"

    def _is_empty_sql_column(self, col: str) -> str:
        """
        Generates a SQL query to check if a column is empty.
        """
        column = self.sql_data_service.pgi.schema.get_column(self.table_id, col)
        if not column:
            raise ValueError(f"Column {col} does not exist in the table {self.table_id}.")

        match column.type:
            case "Char":
                return f"({column.hash} IS NULL OR {column.hash} = '')"
            case "Bool":
                return f"({column.hash} IS NULL)"
            case "Num":
                return f"({column.hash} IS NULL)"
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
            raise ValueError(f"Variable {target} is not a constant.")

        match variable.subtype:
            case "Char":
                return f"(({variable.query}) IS NULL OR ({variable.query}) = '')"
            case "Bool":
                return f"(({variable.query}) IS NULL)"
            case "Num":
                return f"(({variable.query}) IS NULL)"
            case _:
                raise ValueError(f"Unsupported variable type: {variable.subtype} for variable {target}.")
