from cdisc_rules_engine.dataset_builders.values_dataset_builder import (
    ValuesDatasetBuilder,
)


class ContentsLibraryVariablesDatasetBuilder(ValuesDatasetBuilder):
    def build(self):
        """
        Returns a long dataset where each value in each row of the original dataset is
        a row in the new dataset.
        Library metadata corresponding to each row's variable is
        attached to each row.
        Columns available in the dataset are:
        "row_number",
        "variable_name",
        "variable_value",
        "library_variable_name",
        "library_variable_label",
        "library_variable_role",
        "library_variable_ccode",
        ...,
        """
        # get dataset contents and convert it from wide to long
        data_contents_long_df = ValuesDatasetBuilder.build(self)
        variables_metadata = self.get_library_variables_metadata()
        # merge dataset contents with library variable metadata
        merged = data_contents_long_df.merge(
            variables_metadata.data,
            how="outer",
            left_on="variable_name",
            right_on="library_variable_name",
        )
        return merged
