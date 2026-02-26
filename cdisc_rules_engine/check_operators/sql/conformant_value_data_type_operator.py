from .base_sql_operator import BaseSqlOperator
import pandas as pd


class ConformantValueDataTypeOperator(BaseSqlOperator):
    """Operator for checking if values conform to expected data type."""

    def execute_operator(self, other_value):
        # This operator has some implementation in the original version but uses pandas
        results = False
        for vlm in self.value_level_metadata:
            results |= self.validation_df.apply(
                lambda row: vlm["filter"](row) and vlm["type_check"](row),
                axis=1,
                meta=pd.Series([True, False], dtype=bool),
            ).fillna(False)
        return self.validation_df.convert_to_series(results)
