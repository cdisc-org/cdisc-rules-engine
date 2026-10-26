from .base_sql_operator import BaseSqlOperator


class IsInconsistentAcrossDatasetOperator(BaseSqlOperator):
    """Operator for checking if values are inconsistent across dataset."""

    def execute_operator(self, other_value):
        """
        Checks if values in the target column are inconsistent across groups defined by comparator column(s).

        Returns True for rows where the target column has multiple distinct values within the same group,
        False for rows where all values in the group are consistent.
        """
        target = other_value.get("target")
        comparator = other_value.get("comparator")
        where_populated = other_value.get("where_populated", False)
        where_populated_columns = other_value.get("where_populated_columns")
        self._validate_where_populated_args(where_populated, where_populated_columns)

        if not target or not isinstance(target, str) or target in self.operation_variables:
            raise ValueError("Target is required and must be a valid column name.")

        target_column = self.replace_prefix(target).lower()
        if not self._exists(target_column):
            return self._do_check_operator(lambda: "FALSE")

        valid_comparators = self._resolve_valid_comparators(comparator, target_column)
        if len(valid_comparators) == 0:
            return self._do_check_operator(lambda: "FALSE")

        populated_columns = self._where_populated_columns(
            where_populated, where_populated_columns, target_column, valid_comparators
        )

        if len(valid_comparators) == 1:
            return self._handle_single_comparator(target_column, valid_comparators[0], populated_columns)
        else:
            return self._handle_multiple_comparators(target_column, valid_comparators, populated_columns)

    def _validate_where_populated_args(self, where_populated, where_populated_columns):
        if not isinstance(where_populated, bool):
            raise ValueError(
                f"Invalid where_populated type for is_inconsistent_across_dataset operation. "
                f"Expected boolean, got: {type(where_populated).__name__}"
            )

        if where_populated_columns is not None and not isinstance(where_populated_columns, list):
            raise ValueError(
                f"Invalid where_populated_columns type for is_inconsistent_across_dataset operation. "
                f"Expected list of column names, got: {type(where_populated_columns).__name__}"
            )

    def _resolve_valid_comparators(self, comparator, target_column):
        if isinstance(comparator, str):
            comparator_list = [comparator]
        elif isinstance(comparator, list):
            comparator_list = comparator
        else:
            raise ValueError(
                f"Invalid comparator type for is_inconsistent_across_dataset operation on column '{target_column}'. "
                f"Expected string or list of column names, got: {type(comparator).__name__}"
            )

        valid_comparators = []
        for comp in comparator_list:
            comp_col = self.replace_prefix(comp).lower()
            if self._exists(comp_col):
                valid_comparators.append(comp_col)
        return valid_comparators

    def _where_populated_columns(
        self,
        where_populated,
        where_populated_columns,
        target_column,
        comparator_columns,
    ):
        """
        where_populated (bool): if True, requires the target and comparator columns to be populated.
        where_populated_columns (list of column names, optional): requires the named columns to be
            populated, in addition to target/comparator if where_populated is also True.
        Named columns that do not exist in the dataset are ignored. If none of them exist:
        - and where_populated is True, still filter on target/comparator.
        - otherwise, there would be nothing left to filter on, so we raise rather than
          silently falling back to an unfiltered check.
        """
        populated_columns = []
        if where_populated:
            populated_columns.extend([target_column, *comparator_columns])

        if where_populated_columns:
            resolved_columns = []
            for column in where_populated_columns:
                resolved_column = self.replace_prefix(column).lower()
                if self._exists(resolved_column):
                    resolved_columns.append(resolved_column)

            if not resolved_columns and not where_populated:
                raise ValueError(
                    "None of the where_populated_columns exist in the dataset for "
                    f"is_inconsistent_across_dataset operation: {where_populated_columns}"
                )

            for resolved_column in resolved_columns:
                if resolved_column not in populated_columns:
                    populated_columns.append(resolved_column)

        return populated_columns

    def _compiled_inconsistent(self, target_column, comparator_columns, populated_columns):
        target = self._column_sql(target_column, alias=False)
        group_columns = [self._column_sql(column, alias=False) for column in comparator_columns]
        select_groups = ", ".join(f"{column} AS g{index}" for index, column in enumerate(group_columns))
        populated = self._populated_filter_sql(None, populated_columns).removeprefix(" AND ")
        where = f"WHERE {populated}" if populated else ""
        inconsistent_groups = (
            f"SELECT {select_groups}, TRUE AS found FROM {self._table_sql()} {where} "
            f"GROUP BY {', '.join(group_columns)} "
            f"HAVING COUNT(DISTINCT CASE WHEN {target} IS NULL THEN 'NULL_VALUE' ELSE CAST({target} AS TEXT) END) > 1"
        )

        def row_in_group(alias: str) -> str:
            return " AND ".join(
                f"({alias}.g{index} IS NULL) = ({self._column_sql(column)} IS NULL) AND "
                f"COALESCE(CAST({alias}.g{index} AS TEXT), '') = COALESCE(CAST({self._column_sql(column)} AS TEXT), '')"
                for index, column in enumerate(comparator_columns)
            )

        alias = self._join_derived(inconsistent_groups, row_in_group)
        return f"{alias}.found IS NOT NULL{self._populated_filter_sql('co', populated_columns)}"

    def _handle_single_comparator(self, target_column, comparator_column, populated_columns=()):
        compiled = self._compiled_inconsistent(target_column, [comparator_column], populated_columns)
        return self._do_check_operator(lambda: compiled)

    def _handle_multiple_comparators(self, target_column, comparator_columns, populated_columns=()):
        compiled = self._compiled_inconsistent(target_column, comparator_columns, populated_columns)
        return self._do_check_operator(lambda: compiled)

    def _populated_filter_sql(self, table_alias, populated_columns):
        if not populated_columns:
            return ""
        prefix = f"{table_alias}." if table_alias else ""
        return "".join(
            f" AND NULLIF(CAST({prefix}{self._column_sql(column, alias=False)} AS TEXT), '') IS NOT NULL"
            for column in populated_columns
        )
