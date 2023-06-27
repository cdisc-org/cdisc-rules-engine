from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder

import pandas as pd

from cdisc_rules_engine.services import logger


class ContentsDefineDatasetBuilder(BaseDatasetBuilder):
    def build(self, **kwargs):
        """
        Returns a long dataset where each value in each row of the original dataset is
        a row in the new dataset.
        The define xml variable metadata corresponding to each row's variable is
        attached to each row.
        Columns available in the dataset are:

        dataset_size - File size
        dataset_location - Path to file
        dataset_name - Name of the dataset
        dataset_label - Label for the dataset

        Columns from Define XML:
        define_dataset_name - dataset name from define_xml
        define_dataset_label - dataset label from define
        define_dataset_location - dataset location from define
        define_dataset_class - dataset class
        define_dataset_structure - dataset structure
        define_dataset_is_non_standard - whether a dataset is a standard
        define_dataset_variables - dataset variable list

        ...,
        """
        # 1. Build define xml dataframe
        define_col_order = [
            "define_dataset_name",
            "define_dataset_label",
            "define_dataset_location",
            "define_dataset_class",
            "define_dataset_structure",
            "define_dataset_is_non_standard",
            "define_dataset_variables",
        ]
        define_df = pd.DataFrame(kwargs.get("define_metadata", {}))
        if define_df.empty:
            define_df = pd.DataFrame(columns=define_col_order)
            logger.info(f"No define_metadata is provided for {__name__}.")

        # 2. Build dataset dataframe
        dataset_col_order = [
            "dataset_size",
            "dataset_location",
            "dataset_name",
            "dataset_label",
            "variables",
        ]
        datasets = pd.DataFrame(kwargs.get("datasets", {}))

        if datasets.empty or len(datasets) == 0:
            dataset_df = pd.DataFrame(columns=dataset_col_order)
            logger.info(f"No datasets metadata is provided for {__name__}.")
        else:
            data_col_mapping = {
                "filename": "dataset_location",
                "label": "dataset_label",
                "domain": "dataset_name",
            }
            dataset_df = datasets.rename(columns=data_col_mapping).drop(
                "records", axis=1
            )
            dataset_df["dataset_size"] = None
            dataset_df = dataset_df[dataset_col_order]

        # 3. Merge the two data frames
        merged = pd.merge(
            dataset_df,
            define_df,
            how="outer",
            left_on="dataset_name",
            right_on="define_dataset_name",
        )

        # # 4. Add message column
        # merged.insert(2, "Message", None)
        # merged.loc[merged["define_dataset_name"].isnull(), "Message"
        #    ] = "In dataset but not in define: " + merged["domain"].astype(str)
        # merged.loc[merged["domain"].isnull(), "Message"
        #    ] = "In define but not in dataset: "
        #    + merged["define_dataset_name"].astype(str)
        # merged.loc[
        #     (merged["define_dataset_name"].notnull()) & (merged["domain"].notnull()) &
        #     (merged["define_dataset_name"] == merged["domain"]),
        #     "Message"
        # ] = "Matched: " + merged["domain"].astype(str)

        # 5. Replace Nan with None
        # outer join, so some data contents may be missing or some define metadata may
        # be missing. Replace nans with None
        merged_no_nans = merged.where(pd.notnull(merged), None)
        return merged_no_nans
