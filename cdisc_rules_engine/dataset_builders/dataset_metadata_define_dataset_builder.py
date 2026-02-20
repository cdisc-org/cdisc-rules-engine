from cdisc_rules_engine.services import logger
from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)
import numpy as np


class DatasetMetadataDefineDatasetBuilder(BaseDatasetBuilder):
    def build(self):
        """
        Returns a dataset where each dataset is a row in the new dataset.
        The define xml dataset metadata is attached to each row.
        Columns available in the dataset are:

        dataset_size - File size
        dataset_location - Path to file
        dataset_name - Name of the dataset
        dataset_label - Label for the dataset
        dataset_domain - Domain of the dataset
        dataset_columns - List of columns in the dataset
        is_ap - Whether the domain is an AP domain
        ap_suffix - The 2-character suffix from AP domains

        Columns from Define XML:
        define_dataset_name - dataset name from define_xml
        define_dataset_label - dataset label from define
        define_dataset_location - dataset location from define
        define_dataset_domain - dataset domain from define
        define_dataset_class - dataset class
        define_dataset_structure - dataset structure
        define_dataset_is_non_standard - whether a dataset is a standard
        define_dataset_variables - list of variables in the dataset
        define_dataset_variable_order - ordered list of variables in the dataset
        (ordered by OrderNumber if present, otherwise by XML document order)
        define_dataset_key_sequence - ordered list of key sequence variables in the dataset
        define_dataset_has_no_data - whether a dataset has no data

        ...,
        """
        # 1. Build define xml dataframe
        define_df = self._get_define_xml_dataframe()

        # 2. Build dataset dataframe
        dataset_df = self._get_dataset_dataframe()
        if define_df.empty or dataset_df.empty:
            raise ValueError(
                "DatasetMetadataDefineDatasetBuilder: Define or Dataset metadata is empty."
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

        # 5. Return all rows (one per dataset)
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
            "define_dataset_variables",
            "define_dataset_variable_order",
            "define_dataset_key_sequence",
            "define_dataset_has_no_data",
        ]
        define_metadata = self.get_define_metadata()
        if not define_metadata:
            logger.info(f"No define_metadata is provided for {__name__}.")
            return self.dataset_implementation(columns=define_col_order)
        define_xml_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.dataset_path, self.define_xml_path, self.data_service, self.cache
        )
        enriched_metadata = []
        for basic_metadata in define_metadata:
            dataset_name = basic_metadata.get("define_dataset_name")
            if dataset_name:
                try:
                    full_metadata = define_xml_reader.extract_dataset_metadata(
                        dataset_name
                    )
                    enriched_metadata.append(full_metadata)
                except Exception as e:
                    logger.trace(e)
                    logger.error(
                        f"Error extracting metadata for {dataset_name}: {str(e)}"
                    )
                    basic_metadata["define_dataset_variables"] = None
                    basic_metadata["define_dataset_variable_order"] = None
                    basic_metadata["define_dataset_key_sequence"] = None
                    basic_metadata["define_dataset_has_no_data"] = None
                    enriched_metadata.append(basic_metadata)
            else:
                basic_metadata["define_dataset_variables"] = None
                basic_metadata["define_dataset_variable_order"] = None
                basic_metadata["define_dataset_key_sequence"] = None
                basic_metadata["define_dataset_has_no_data"] = None
                enriched_metadata.append(basic_metadata)
        return self.dataset_implementation.from_records(enriched_metadata)

    def _ensure_required_columns(self, dataset_df, dataset_col_order):
        if "dataset_size" not in dataset_df.columns:
            dataset_df["dataset_size"] = None
        if "is_ap" not in dataset_df.columns:
            dataset_df["is_ap"] = False
        if "dataset_columns" not in dataset_df.columns:
            dataset_df["dataset_columns"] = None
        if "ap_suffix" not in dataset_df.columns:
            dataset_df["ap_suffix"] = ""
        return self.dataset_implementation(dataset_df[dataset_col_order])

    def _get_dataset_dataframe(self):
        dataset_col_order = [
            "dataset_size",
            "dataset_location",
            "dataset_name",
            "dataset_label",
            "dataset_domain",
            "dataset_columns",
            "is_ap",
            "ap_suffix",
        ]

        if len(self.datasets) == 0:
            dataset_df = self.dataset_implementation(columns=dataset_col_order)
            logger.info(f"No datasets metadata is provided in {__name__}.")
        else:
            datasets = self.dataset_implementation()
            for dataset in self.datasets:
                ds_metadata = None
                try:
                    ds_metadata = self.data_service.get_dataset_metadata(
                        dataset_name=dataset.filename
                    )
                    ds_metadata.data["dataset_domain"] = getattr(
                        dataset, "domain", None
                    )
                    if dataset.first_record:
                        ds_metadata.data["dataset_columns"] = [
                            list(dataset.first_record.keys())
                        ]
                    else:
                        ds_metadata.data["dataset_columns"] = [[]]
                except Exception as e:
                    logger.trace(e)
                    logger.error(f"Error: {e}. Error message: {str(e)}")
                if ds_metadata:
                    if datasets.data.empty:
                        datasets.data = ds_metadata.data.copy()
                    else:
                        datasets.data = datasets.concat(ds_metadata).data
            if datasets.data.empty or len(datasets.data) == 0:
                dataset_df = self.dataset_implementation(columns=dataset_col_order)
                logger.info(f"No datasets metadata is provided for {__name__}.")
            else:
                data_col_mapping = {
                    "filename": "dataset_location",
                    "label": "dataset_label",
                }
                dataset_df = datasets.rename(columns=data_col_mapping)
                dataset_df = self._ensure_required_columns(
                    dataset_df, dataset_col_order
                )
        return dataset_df
