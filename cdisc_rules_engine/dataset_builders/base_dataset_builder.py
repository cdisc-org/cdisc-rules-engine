from abc import abstractmethod
from cdisc_rules_engine.constants.define_xml_constants import DEFINE_XML_FILE_NAME
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)
import pandas as pd
from cdisc_rules_engine.utilities.utils import (
    get_directory_path,
    is_split_dataset,
    get_corresponding_datasets,
)
from typing import List
import os
from cdisc_rules_engine.services import logger


class BaseDatasetBuilder:
    def __init__(
        self,
        rule,
        data_service,
        cache_service,
        rule_processor,
        data_processor,
        dataset_path,
        datasets,
        domain,
        define_metadata: List[dict] = [],
    ):
        self.data_service = data_service
        self.cache = cache_service
        self.data_processor = data_processor
        self.rule_processor = rule_processor
        self.dataset_path = dataset_path
        self.datasets = datasets
        self.domain = domain
        self.rule = rule
        self.define_metadata = define_metadata

    @abstractmethod
    def build(self) -> pd.DataFrame:
        """
        Returns correct dataframe to operate on
        """
        pass

    def get_dataset(self, **kwargs):
        # If validating dataset content, ensure split datasets are handled.
        if is_split_dataset(self.datasets, self.domain):
            # Handle split datasets for content checks.
            # A content check is any check that is not in the list of rule types
            dataset: pd.DataFrame = self.data_service.concat_split_datasets(
                func_to_call=self.build,
                dataset_names=self.get_corresponding_datasets_names(),
                **kwargs,
            )
        else:
            # single dataset. the most common case
            dataset: pd.DataFrame = self.build()
        return dataset

    def get_corresponding_datasets_names(self) -> List[str]:
        directory_path = get_directory_path(self.dataset_path)
        return [
            os.path.join(directory_path, dataset["filename"])
            for dataset in get_corresponding_datasets(self.datasets, self.domain)
        ]

    def get_define_xml_item_group_metadata(self, domain: str) -> List[dict]:
        """
        Gets Define XML item group metadata
        returns a list of dictionaries containing the following keys:
            "define_dataset_name"
            "define_dataset_label"
            "define_dataset_location"
            "define_dataset_class"
            "define_dataset_structure"
            "define_dataset_is_non_standard"
            "define_dataset_variables"
        """
        directory_path = get_directory_path(self.dataset_path)
        define_xml_path: str = os.path.join(directory_path, DEFINE_XML_FILE_NAME)
        define_xml_contents: bytes = self.data_service.get_define_xml_contents(
            dataset_name=define_xml_path
        )
        define_xml_reader = DefineXMLReaderFactory.from_file_contents(
            define_xml_contents, cache_service_obj=self.cache
        )
        return define_xml_reader.extract_domain_metadata(domain)

    def get_define_xml_variables_metadata(self) -> List[dict]:
        """
        Gets Define XML variables metadata.
        """
        directory_path = get_directory_path(self.dataset_path)
        define_xml_path: str = os.path.join(directory_path, DEFINE_XML_FILE_NAME)
        define_xml_contents: bytes = self.data_service.get_define_xml_contents(
            dataset_name=define_xml_path
        )
        define_xml_reader = DefineXMLReaderFactory.from_file_contents(
            define_xml_contents, cache_service_obj=self.cache
        )
        return define_xml_reader.extract_variables_metadata(domain_name=self.domain)

    def get_define_xml_value_level_metadata(self) -> List[dict]:
        """
        Gets Define XML value level metadata and returns it as dataframe.
        """
        directory_path = get_directory_path(self.dataset_path)
        define_xml_path: str = os.path.join(directory_path, DEFINE_XML_FILE_NAME)
        define_xml_contents: bytes = self.data_service.get_define_xml_contents(
            dataset_name=define_xml_path
        )
        define_xml_reader = DefineXMLReaderFactory.from_file_contents(
            define_xml_contents, cache_service_obj=self.cache
        )
        return define_xml_reader.extract_value_level_metadata(domain_name=self.domain)

    @staticmethod
    def add_row_number(dataframe: pd.DataFrame) -> None:
        dataframe["row_number"] = range(1, len(dataframe) + 1)

    def get_dataset_define_metadata(self, **kwargs):
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

        Columns from Define XML are:
        define_dataset_name - dataset name from define_xml
        define_dataset_label - dataset label from define
        define_dataset_location - dataset location from define
        define_dataset_class - dataset class
        define_dataset_structure - dataset structure
        define_dataset_is_non_standard - whether a dataset is a standard
        define_dataset_variables - dataset variable list

        ...,
        """

        v_prg = "get_dataset_define_metadata"
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
        define_df = pd.DataFrame(self.define_metadata)
        if define_df.empty:
            define_df = pd.DataFrame(columns=define_col_order)
            logger.info(f"No define_metadata is provided for {__name__}.")

        # 2. Build dataset dataframe
        dataset_col_order = [
            "dataset_size",
            "dataset_location",
            "dataset_name",
            "dataset_label",
        ]

        if len(self.datasets) == 0:
            dataset_df = pd.DataFrame(columns=dataset_col_order)
            logger.info(f"No datasets metadata is provided in {v_prg}.")
        else:
            datasets = pd.DataFrame()
            for dataset in self.datasets:
                try:
                    ds_metadata = self.data_service.get_dataset_metadata(
                        dataset["filename"]
                    )
                except Exception as e:
                    logger._exception = e
                    logger.error()
                datasets = (
                    ds_metadata if datasets.empty else datasets.append(ds_metadata)
                )

            if datasets.empty or len(datasets) == 0:
                dataset_df = pd.DataFrame(columns=dataset_col_order)
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
                dataset_df = dataset_df[dataset_col_order]

        # 3. Merge the two data frames
        merged = pd.merge(
            dataset_df,
            define_df,
            how="outer",
            left_on="dataset_name",
            right_on="define_dataset_name",
        )

        # 4. Replace Nan with None
        # outer join, so some data contents may be missing or some define metadata may
        # be missing. Replace nans with None
        merged_no_nans = merged.where(pd.notnull(merged), None)
        return merged_no_nans
