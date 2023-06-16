from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
from cdisc_rules_engine.services.define_xml.base_define_xml_reader import (
    BaseDefineXMLReader,
)

# from cdisc_rules_engine.utilities.utils import is_split_dataset
import pandas as pd
from typing import List


class ContentsDefineDatasetBuilder(BaseDatasetBuilder):
    def build(self, **kwargs):
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
        print(f"xxx: kwargs: {kwargs}")
        xml_reader = BaseDefineXMLReader()

        print(f"xxx: (xml_reader): {xml_reader}")

        # get dataset contents and convert it from wide to long
        data_contents_df = self.data_service.get_dataset(dataset_name=self.dataset_path)
        print(f"xxx: (Dataset1): {data_contents_df} in {__name__}")
        data_contents_df["row_number"] = range(1, len(data_contents_df) + 1)
        data_contents_long_df = pd.melt(
            data_contents_df,
            id_vars="row_number",
            var_name="variable_name",
            value_name="variable_value",
        )
        print(f"xxx: (Dataset2): {data_contents_long_df} in {__name__}")
        # get Define XML variable metadata for domain
        variables_metadata: List[dict] = self.get_define_xml_variables_metadata(
            **kwargs
        )
        print(f"xxx: (Var Metadata 1): {variables_metadata} in {__name__}")
        variables_metadata_df = pd.DataFrame(variables_metadata)
        print(f"xxx: (Var Metadata 2): {variables_metadata_df} in {__name__}")
        # merge dataset contents with define variable metadata
        merged = pd.merge(
            data_contents_long_df,
            variables_metadata_df,
            how="outer",
            left_on="variable_name",
            right_on="define_variable_name",
        )
        print(f"xxx: (Merged 1): {merged} in {__name__}")
        # outer join, so some data contents may be missing or some define metadata may
        # be missing. Replace nans with None
        merged_no_nans = merged.where(pd.notnull(merged), None)
        print(f"xxx: (Merged 2): {merged_no_nans} in {__name__}")
        return merged_no_nans

    def get_domain_name():
        return
