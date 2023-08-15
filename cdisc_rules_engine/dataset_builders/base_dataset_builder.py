from abc import abstractmethod
import pandas as pd
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)
from cdisc_rules_engine.utilities.utils import (
    get_directory_path,
    is_split_dataset,
    get_corresponding_datasets,
)
from typing import List
from cdisc_rules_engine import config
from cdisc_rules_engine.utilities import sdtm_utilities
import os


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
        define_xml_path,
        standard,
        standard_version,
    ):
        self.data_service = data_service
        self.cache = cache_service
        self.data_processor = data_processor
        self.rule_processor = rule_processor
        self.dataset_path = dataset_path
        self.datasets = datasets
        self.domain = domain
        self.rule = rule
        self.define_xml_path = define_xml_path
        self.standard = standard
        self.standard_version = standard_version

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

    def get_dataset_contents(self, **kwargs):
        # If validating dataset content, ensure split datasets are handled.
        if is_split_dataset(self.datasets, self.domain):
            # Handle split datasets for content checks.
            # A content check is any check that is not in the list of rule types
            dataset: pd.DataFrame = self.data_service.concat_split_datasets(
                func_to_call=self.data_service.get_dataset,
                dataset_names=self.get_corresponding_datasets_names(),
                **kwargs,
            )
        else:
            # single dataset. the most common case
            dataset: pd.DataFrame = self.data_service.get_dataset(self.dataset_path)
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

        define_xml_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.dataset_path, self.define_xml_path, self.data_service, self.cache
        )
        return define_xml_reader.extract_domain_metadata(domain)

    def get_define_xml_variables_metadata(self) -> List[dict]:
        """
        Gets Define XML variables metadata.
        """
        define_xml_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.dataset_path, self.define_xml_path, self.data_service, self.cache
        )
        return define_xml_reader.extract_variables_metadata(domain_name=self.domain)

    def get_define_xml_value_level_metadata(self) -> List[dict]:
        """
        Gets Define XML value level metadata and returns it as dataframe.
        """
        define_xml_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.dataset_path, self.define_xml_path, self.data_service, self.cache
        )
        return define_xml_reader.extract_value_level_metadata(domain_name=self.domain)

    @staticmethod
    def add_row_number(dataframe: pd.DataFrame) -> None:
        dataframe["row_number"] = range(1, len(dataframe) + 1)

    def get_define_metadata(self):
        define_xml_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.dataset_path, self.define_xml_path, self.data_service, self.cache
        )
        return define_xml_reader.read()

    def get_library_variables_metadata(self) -> pd.DataFrame:
        # TODO: Update to support other standard types
        variables: List[dict] = sdtm_utilities.get_variables_metadata_from_standard(
            standard=self.standard,
            standard_version=self.standard_version,
            domain=self.domain,
            cache=self.cache,
            config=config,
        )

        # Rename columns:
        column_name_mapping = {
            "ordinal": "order_number",
            "simpleDatatype": "data_type",
        }

        for var in variables:
            var["name"] = var["name"].replace("--", self.domain)
            for key, new_key in column_name_mapping.items():
                var[new_key] = var.pop(key)

        return pd.DataFrame(variables).add_prefix("library_variable_")
