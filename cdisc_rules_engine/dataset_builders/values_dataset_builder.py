from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
import pandas as pd


class ValuesDatasetBuilder(BaseDatasetBuilder):
    def build(self):
        """
        Returns a long dataset where each value in each row of the original dataset is
        a row in the new dataset.
        Columns available in the dataset are:
        "row_number",
        "variable_name",
        "variable_value"
        ...,
        """
        data_contents_df: pd.DataFrame = self.data_service.get_dataset(
            dataset_name=self.dataset_path
        )
        self.add_row_number(data_contents_df)
        values_df: pd.DataFrame = pd.melt(
            data_contents_df,
            id_vars="row_number",
            var_name="variable_name",
            value_name="variable_value",
        )
        return values_df
