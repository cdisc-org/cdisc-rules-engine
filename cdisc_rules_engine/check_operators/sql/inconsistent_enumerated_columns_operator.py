from .base_sql_operator import BaseSqlOperator


class InconsistentEnumeratedColumnsOperator(BaseSqlOperator):
    """Operator for checking inconsistent enumerated columns."""

    def execute_operator(self, other_value):
        """
        Check for inconsistencies in enumerated columns of a DataFrame.

        Starting with the smallest/largest enumeration of the given variable,
        return an error if VARIABLE(N+1) is populated but VARIABLE(N) is not populated.
        Repeat for all variables belonging to the enumeration.
        Note that the initial variable will not have an index (VARIABLE) and
        the next enumerated variable has index 1 (VARIABLE1).
        """
        """variable_name: str = self.replace_prefix(other_value.get("target"))
        df = self.validation_df"""
        # pattern = rf"^{re.escape(variable_name)}(\d*)$"
        """matching_columns = [col for col in df.columns if re.match(pattern, col)]
        if not matching_columns:
            return pd.Series([False] * len(df))  # Return a series of False values if no matching columns
        sorted_columns = sorted(matching_columns, key=lambda x: (len(x), x))

        def check_inconsistency(row):
            prev_populated = pd.notna(row[sorted_columns[0]]) and row[sorted_columns[0]] != ""
            for i in range(1, len(sorted_columns)):
                curr_col = sorted_columns[i]
                curr_value = row[curr_col]
                if pd.notna(curr_value) and curr_value != "" and not prev_populated:
                    return True
                prev_populated = pd.notna(curr_value) and curr_value != ""
            return False

        return df.apply(check_inconsistency, axis=1)"""
        raise NotImplementedError("inconsistent_enumerated_columns check_operator not implemented")
