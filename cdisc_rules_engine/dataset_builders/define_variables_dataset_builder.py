from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
from typing import List


class DefineVariablesDatasetBuilder(BaseDatasetBuilder):
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
        """
        # get Define XML metadata for domain and use it as a rule comparator
        variable_metadata: List[dict] = self.get_define_xml_variables_metadata()
        return self.dataset_implementation(variable_metadata)
