from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
from cdisc_rules_engine.models.dataset import DatasetInterface


class VariablesMetadataWithLibraryMetadataDatasetBuilder(BaseDatasetBuilder):
    def build(self):
        """
        Returns the variable metadata from a given file.
        Returns a dataframe with the following columns:
        variable_name
        variable_order_number
        variable_label
        variable_size
        variable_data_type
        variable_has_empty_values
        library_variable_name,
        library_variable_label,
        library_variable_data_type,
        library_variable_role,
        library_variable_core,
        library_variable_order_number
        """
        # get dataset metadata and execute the rule
        content_variables_metadata: DatasetInterface = (
            self.data_service.get_variables_metadata(
                dataset_name=self.dataset_path,
                datasets=self.datasets,
                drop_duplicates=True,
            )
        )
        dataset_contents = self.get_dataset_contents()
        library_variables_metadata = self.get_library_variables_metadata()

        data = content_variables_metadata.merge(
            library_variables_metadata.data,
            how="outer",
            left_on="variable_name",
            right_on="library_variable_name",
        ).fillna("")

        data["variable_has_empty_values"] = data.apply(
            lambda row: self.variable_has_null_values(
                row["variable_name"], dataset_contents
            ),
            axis=1,
        )
        return data

    def variable_has_null_values(
        self, variable: str, content: DatasetInterface
    ) -> bool:
        if variable not in content:
            return True
        series = content[variable]
        return series.mask(series == "").isnull().any()
