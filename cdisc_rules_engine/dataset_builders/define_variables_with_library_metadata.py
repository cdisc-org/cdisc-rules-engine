from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder


class DefineVariablesWithLibraryMetadataDatasetBuilder(BaseDatasetBuilder):
    def build(self):
        """
        Returns a dataset containing metadata for the variables
        in the specified domain extracted from the define.xml.
        Columns available in the dataset are:
        "define_variable_name",
        "define_variable_label",
        "define_variable_data_type",
        "define_variable_role",
        "define_variable_size",
        "define_variable_ccode",
        "define_variable_format",
        "define_variable_allowed_terms",
        "define_variable_origin_type",
        "define_variable_is_collected",
        "define_variable_has_no_data",
        "define_variable_order_number",
        "define_variable_has_codelist",
        "define_variable_codelist_coded_values",
        "define_variable_mandatory",
        "define_variable_has_comment",
        "library_variable_name",
        "library_variable_label",
        "library_variable_data_type",
        "library_variable_role",
        "library_variable_core",
        "library_variable_order_number"
        """
        # get Define XML metadata for domain and use it as a rule comparator
        variable_metadata = self.dataset_implementation.from_records(
            self.get_define_xml_variables_metadata()
        )
        library_variables_metadata = self.get_library_variables_metadata()

        data = variable_metadata.merge(
            library_variables_metadata.data,
            how="outer",
            left_on="define_variable_name",
            right_on="library_variable_name",
        ).data.fillna("")
        return self.dataset_implementation(data)
