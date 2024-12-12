from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
from typing import List
from cdisc_rules_engine.models.dataset import DatasetInterface


class VariablesMetadataWithDefineAndLibraryDatasetBuilder(BaseDatasetBuilder):
    def build(self):
        """
        Returns the variable metadata from a given file.
        Returns a dataframe with the following columns:
        variable_name
        variable_order_number
        variable_label
        variable_size
        variable_data_type
        define_variable_name,
        define_variable_label,
        define_variable_data_type,
        define_variable_role,
        define_variable_size,
        define_variable_code,
        define_variable_format,
        define_variable_allowed_terms,
        define_variable_origin_type,
        define_variable_is_collected,
        define_variable_has_no_data,
        define_variable_order_number,
        define_variable_has_codelist,
        define_variable_codelist_coded_values
        define_variable_mandatory
        library_variable_name,
        library_variable_label,
        library_variable_data_type,
        library_variable_role,
        library_variable_core,
        library_variable_order_number
        """
        variable_metadata: List[dict] = self.get_define_xml_variables_metadata()
        content_metadata: DatasetInterface = self.data_service.get_variables_metadata(
            dataset_name=self.dataset_path, datasets=self.datasets, drop_duplicates=True
        )
        define_metadata: DatasetInterface = self.dataset_implementation.from_records(
            variable_metadata
        )
        library_metadata: DatasetInterface = self.get_library_variables_metadata()
        dataset_contents = self.get_dataset_contents()

        # First merge: content metadata with define metadata
        merged_data = content_metadata.merge(
            define_metadata.data,
            left_on="variable_name",
            right_on="define_variable_name",
            how="outer",
        )
        # Second merge: add library metadata
        final_dataframe = merged_data.merge(
            library_metadata.data,
            how="outer",
            left_on="variable_name",
            right_on="library_variable_name",
        ).fillna("")

        final_dataframe["variable_has_empty_values"] = final_dataframe.apply(
            lambda row: self.variable_has_null_values(
                row["variable_name"]
                if row["variable_name"] != ""
                else row["library_variable_name"],
                dataset_contents,
            ),
            axis=1,
        )

        return final_dataframe

    def variable_has_null_values(
        self, variable: str, content: DatasetInterface
    ) -> bool:
        if variable not in content:
            return True
        series = content[variable]
        return series.mask(series == "").isnull().any()
