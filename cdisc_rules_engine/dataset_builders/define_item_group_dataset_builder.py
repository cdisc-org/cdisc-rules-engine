from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
import pandas as pd
from typing import List


class DefineItemGroupDatasetBuilder(BaseDatasetBuilder):
    def build(self):
        """
        Returns a dataset containing metadata for the domains
        extracted from the define.xml.
        Columns available in the dataset are:
            "define_dataset_name"
            "define_dataset_label"
            "define_dataset_location"
            "define_dataset_class"
            "define_dataset_structure"
            "define_dataset_is_non_standard"
            "define_dataset_variables"
        """
        item_group_metadata: List[dict] = self.get_define_xml_item_group_metadata(
            self.domain
        )
        return pd.DataFrame([item_group_metadata])
