from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
from typing import List
from cdisc_rules_engine.models.dataset import DatasetInterface


class VariablesMetadataWithDefineDatasetBuilder(BaseDatasetBuilder):
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
        define_variable_codelist_coded_values,
        define_variable_codelist_coded_codes,
        define_variable_mandatory,
        define_variable_has_comment,
        define_variable_has_method
        """
        # get Define XML metadata for domain and use it as a rule comparator
        variable_metadata: List[dict] = self.get_define_xml_variables_metadata()
        # get dataset metadata and execute the rule
        content_metadata: DatasetInterface = self.data_service.get_variables_metadata(
            dataset_name=self.dataset_path, datasets=self.datasets, drop_duplicates=True
        )
        define_metadata: DatasetInterface = self.dataset_implementation.from_records(
            variable_metadata
        )

        final_dataframe = content_metadata.merge(
            define_metadata.data,
            left_on="variable_name",
            right_on="define_variable_name",
            how="outer",
        ).fillna("")

        dataset_contents = self.get_dataset_contents()
        final_dataframe[["variable_has_empty_values", "variable_is_empty"]] = (
            final_dataframe.apply(
                lambda row: self.get_variable_null_stats(
                    row["variable_name"], dataset_contents
                ),
                axis=1,
                result_type="expand",
            )
        )

        return final_dataframe

    def get_variable_null_stats(
        self, variable: str, content: DatasetInterface
    ) -> tuple[bool, bool]:
        if variable not in content:
            return True, True
        series = content[variable].mask(content[variable] == "")
        return series.isnull().any(), series.isnull().all()
