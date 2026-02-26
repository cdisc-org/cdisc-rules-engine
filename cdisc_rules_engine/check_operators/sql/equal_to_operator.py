from .base_sql_operator import BaseSqlOperator


class EqualToOperator(BaseSqlOperator):
    """
    Operator for equality comparisons.

    Equality checks work slightly differently for clinical datasets.
    See truth table below:
    Operator       --A         --B         Outcome
    equal_to       "" or null  "" or null  False
    equal_to       "" or null  Populated   False
    equal_to       Populated   "" or null  False
    equal_to       Populated   Populated   A == B
    not_equal_to   "" or null  "" or null  False
    not_equal_to   "" or null  Populated   True
    not_equal_to   Populated   "" or null  True
    not_equal_to   Populated   Populated   A != B
    """

    def __init__(self, data, invert=False, case_insensitive=False):
        super().__init__(data)
        self.invert = invert
        self.case_insensitive = case_insensitive

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target"))
        value_is_literal = other_value.get("value_is_literal", False)
        value_is_reference = other_value.get("value_is_reference", False)
        type_insensitive = other_value.get("type_insensitive", False)
        comparator = other_value.get("comparator")
        if not value_is_literal:
            comparator = self.replace_prefix(comparator)

        if value_is_reference:
            if not self._exists(target):
                raise KeyError(f"Target column '{target}' not found in dataset")
            if not self._exists(comparator):
                raise KeyError(f"Comparator column '{comparator}' not found in dataset")

            return self._check_equality_reference(
                target,
                comparator,
                invert=self.invert,
                case_insensitive=self.case_insensitive,
                type_insensitive=type_insensitive,
            )

        return self._check_equality(
            target,
            comparator,
            invert=self.invert,
            case_insensitive=self.case_insensitive,
            type_insensitive=type_insensitive,
            value_is_literal=value_is_literal,
        )

    def _check_equality(
        self,
        original_target,
        original_comparator,
        invert: bool = False,
        value_is_literal: bool = False,
        case_insensitive: bool = False,
        type_insensitive: bool = False,
    ):
        """
        Beware of empty values.
        See truth table above for details.
        """
        target = self._sql(original_target, lowercase=case_insensitive)
        comparator = self._sql(original_comparator, lowercase=case_insensitive, value_is_literal=value_is_literal)

        if type_insensitive:
            target = f"""CAST({target} AS TEXT)"""
            comparator = f"""CAST({comparator} AS TEXT)"""

        def sql():
            if invert:
                return f"""CASE
                        WHEN {self._is_empty_sql(original_target)}
                            THEN NOT ({self._is_empty_sql(original_comparator)})
                        WHEN {self._is_empty_sql(original_comparator)}
                            THEN TRUE
                        ELSE {target} != {comparator}
                    END"""
            else:
                return f"""CASE
                        WHEN {self._is_empty_sql(original_target)}
                            THEN FALSE
                        WHEN {self._is_empty_sql(original_comparator)}
                            THEN FALSE
                        ELSE {target} = {comparator}
                    END"""

        return self._do_check_operator(sql)

    def _check_equality_reference(
        self,
        original_target,
        pivot_column,
        invert: bool = False,
        case_insensitive: bool = False,
        type_insensitive: bool = False,
    ):
        """
        Beware of empty values.
        See truth table above for details.

        This method implements equality testing by reference, ie you specifiy a pivot
        column, that column is then used to look up which other column to compare
        that row against. The way we handle that in SQL is by finding out all of the
        columns that could be referenced (the DISTINCT values of the pivot column),
        and then generating a CASE statement that checks each of those values.
        """
        target_col = self._column_sql(original_target, lowercase=case_insensitive, alias=True)
        pivot_col = self._column_sql(pivot_column, alias=False)

        if type_insensitive:
            target_col = f"""CAST({target_col} AS TEXT)"""

        # Find all of the values of the pivot column -> all columns to compare against
        self.sql_data_service.pgi.execute_sql(f"SELECT DISTINCT {pivot_col} col FROM {self._table_sql()};")
        comparison_values = self.sql_data_service.pgi.fetch_all()
        comparison_values = [item["col"].lower() for item in comparison_values]
        comparison_values = filter(self._exists, comparison_values)

        # This builds up the case statement for a simple column comparison
        def single_comparison_sql(original_c):
            c = self._column_sql(original_c, lowercase=case_insensitive, alias=True)

            if type_insensitive:
                c = f"""CAST({c} AS TEXT)"""

            if invert:
                return f"""CASE
                        WHEN {self._is_empty_sql(original_target)}
                            THEN {self._is_empty_sql(original_c)}
                        WHEN {self._is_empty_sql(original_c)}
                            THEN TRUE
                        ELSE {target_col} != {c}
                    END"""
            else:
                return f"""CASE
                        WHEN {self._is_empty_sql(original_target)}
                            THEN FALSE
                        WHEN {self._is_empty_sql(original_c)}
                            THEN FALSE
                        ELSE {target_col} = {c}
                    END"""

        def sql():
            sql = "CASE "
            # Build a CASE statement for each possible column
            for c in comparison_values:
                sql += f"WHEN LOWER({pivot_col}) = '{c.lower()}' THEN ({single_comparison_sql(c)}) "
            sql += "ELSE FALSE END"
            # Getting SQL syntax errors if comparison_values is empty, so catching here and returning False
            if sql == "CASE ELSE FALSE END":
                sql = "CASE WHEN 1=1 THEN FALSE END"
            return sql

        return self._do_check_operator(sql)
