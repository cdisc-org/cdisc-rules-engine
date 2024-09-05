from cdisc_rules_engine.services import logger
from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
import os
import numpy as np


class ContentsDefineDatasetBuilder(BaseDatasetBuilder):
    def build(self):
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
        dataset_domain - Domain of the dataset

        Columns from Define XML:
        define_dataset_name - dataset name from define_xml
        define_dataset_label - dataset label from define
        define_dataset_location - dataset location from define
        define_dataset_domain - dataset domain from define
        define_dataset_class - dataset class
        define_dataset_structure - dataset structure
        define_dataset_is_non_standard - whether a dataset is a standard

        ...,
        """
        # 1. Build define xml dataframe
        define_df = self._get_define_xml_dataframe()
        # )
        # 2. Build dataset dataframe
        dataset_df = self._get_dataset_dataframe()
        if define_df.empty or dataset_df.empty:
            raise ValueError(
                "ContentsDefineDatasetBuilder: Define or Dataset metadata is empty."
            )
        # 3. Merge the two data frames
        merged = dataset_df.merge(
            define_df.data,
            left_on=["dataset_name", "dataset_location"],
            right_on=["define_dataset_name", "define_dataset_location"],
            how="outer",
        )
        # 4. Remove NaN
        merged._data = merged._data.astype(object).replace({np.nan: None})
        # 5. remove unused rows, replace rows with target row
        merged_cleaned = merged.dropna(subset=["dataset_name"])
        dataset_filename = (
            os.path.basename(self.dataset_path).lower() if self.dataset_path else None
        )
        matching_row = merged_cleaned[
            merged_cleaned["dataset_location"].str.lower() == dataset_filename
        ]
        for column in merged.columns:
            merged[column] = matching_row[column].iloc[0]
        return merged

    def _get_define_xml_dataframe(self):
        define_col_order = [
            "define_dataset_name",
            "define_dataset_label",
            "define_dataset_location",
            "define_dataset_domain",
            "define_dataset_class",
            "define_dataset_structure",
            "define_dataset_is_non_standard",
        ]
        define_metadata = self.get_define_metadata()
        if not define_metadata:
            logger.info(f"No define_metadata is provided for {__name__}.")
            return self.dataset_implementation(columns=define_col_order)
        return self.dataset_implementation.from_records(define_metadata)

    def _get_dataset_dataframe(self):
        dataset_col_order = [
            "dataset_size",
            "dataset_location",
            "dataset_name",
            "dataset_label",
            "dataset_domain",
        ]

        if len(self.datasets) == 0:
            dataset_df = self.dataset_implementation(columns=dataset_col_order)
            logger.info(f"No datasets metadata is provided in {__name__}.")
        else:
            datasets = self.dataset_implementation()
            for dataset in self.datasets:
                try:
                    ds_metadata = self.data_service.get_dataset_metadata(
                        dataset["filename"]
                    )
                    ds_metadata.data["dataset_domain"] = dataset.get("domain", None)
                except Exception as e:
                    logger.trace(e, __name__)
                    logger.error(f"Error: {e}. Error message: {str(e)}")
                datasets.data = (
                    ds_metadata.data
                    if datasets.data.empty
                    else datasets.data.append(ds_metadata.data)
                )

            if datasets.data.empty or len(datasets.data) == 0:
                dataset_df = self.dataset_implementation(columns=dataset_col_order)
                logger.info(f"No datasets metadata is provided for {__name__}.")
            else:
                data_col_mapping = {
                    "filename": "dataset_location",
                    "label": "dataset_label",
                    "domain": "dataset_name",
                }
                dataset_df = datasets.rename(columns=data_col_mapping)
                if "dataset_size" not in dataset_df.columns:
                    dataset_df["dataset_size"] = None
                dataset_df = self.dataset_implementation(dataset_df[dataset_col_order])
        return dataset_df
