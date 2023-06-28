from cdisc_rules_engine.dataset_builders.values_dataset_builder import (
    ValuesDatasetBuilder,
)
import pandas as pd
from typing import List


class ContentsDefineVariablesDatasetBuilder(ValuesDatasetBuilder):
    def build(self):
        """
        Returns a long dataset where each value in each row of the original dataset is
        a row in the new dataset.
        The define xml variable metadata corresponding to each row's variable is
        attached to each row.
        Columns available in the dataset are:
        "row_number",
        "variable_name",
        "variable_value",
        "define_variable_name",
        "define_variable_label",
        "define_variable_data_type",
        "define_variable_role",
        ...,
        """
        # get dataset contents and convert it from wide to long
        data_contents_long_df = ValuesDatasetBuilder.build(self)
        # get Define XML variable metadata for domain
        variables_metadata: List[dict] = self.get_define_xml_variables_metadata()
        variables_metadata_df = pd.DataFrame(variables_metadata)
        # merge dataset contents with define variable metadata
        merged = pd.merge(
            data_contents_long_df,
            variables_metadata_df,
            how="outer",
            left_on="variable_name",
            right_on="define_variable_name",
        )
        # outer join, so some data contents may be missing or some define metadata may
        # be missing. Replace nans with None
        merged_no_nans = merged.where(pd.notnull(merged), None)
        return merged_no_nans
