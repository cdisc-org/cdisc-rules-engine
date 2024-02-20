from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
from cdisc_rules_engine.models.dataset import DatasetInterface


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
        data_contents_df: DatasetInterface = self.data_service.get_dataset(
            dataset_name=self.dataset_path
        )
        self.add_row_number(data_contents_df)
        values_df: DatasetInterface = data_contents_df.melt(
            id_vars="row_number",
            var_name="variable_name",
            value_name="variable_value",
        )
        return values_df

    @staticmethod
    def calculate_variable_value_length(variable_value, variable_data_type: str) -> int:
        if variable_data_type == "integer":
            return len(str(variable_value).lstrip("0"))
        elif variable_data_type == "float":
            return len(str(variable_value).lstrip("0").replace(".", ""))
        elif variable_data_type == "text":
            return len(variable_value)
        return None
