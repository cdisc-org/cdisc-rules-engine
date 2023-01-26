from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
from typing import List
import pandas as pd


class VariablesMetadataWithDefineDatasetBuilder(BaseDatasetBuilder):
    def build(self):
        """
        Returns the variable metadata from a given file.
        Returns a dataframe with the following columns:
        variable_name
        variable_order
        variable_label
        variable_size
        variable_data_type
        define_variable_name,
        define_variable_label,
        define_variable_data_type,
        define_variable_role,
        define_variable_size,
        define_variable_ccode,
        define_variable_format,
        define_variable_allowed_terms,
        define_variable_origin_type,

        """
        # get Define XML metadata for domain and use it as a rule comparator
        variable_metadata: List[dict] = self.get_define_xml_variables_metadata()
        # get dataset metadata and execute the rule
        content_metadata: pd.DataFrame = self.data_service.get_variables_metadata(
            self.dataset_path, drop_duplicates=True
        )
        define_metadata: pd.DataFrame = pd.DataFrame(variable_metadata)
        return content_metadata.merge(
            define_metadata,
            left_on="variable_name",
            right_on="define_variable_name",
            how="left",
        )
