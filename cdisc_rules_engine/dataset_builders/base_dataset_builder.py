from abc import abstractmethod
from cdisc_rules_engine.constants.define_xml_constants import DEFINE_XML_FILE_NAME
from cdisc_rules_engine.services.define_xml_reader import DefineXMLReader
import pandas as pd
from cdisc_rules_engine.utilities.utils import (
    get_directory_path,
    get_corresponding_datasets,
)
from typing import List


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
    ):
        self.data_service = data_service
        self.cache = (cache_service,)
        self.data_processor = data_processor
        self.rule_processor = rule_processor
        self.dataset_path = dataset_path
        self.datasets = datasets
        self.domain = domain
        self.rule = rule

    @abstractmethod
    def build(self) -> pd.DataFrame:
        """
        Returns correct dataframe to operate on
        """
        pass

    def get_dataset(self, **kwargs):
        # By default do not handle split datasets
        # This method is overridden in the ContentDatasetBuilder for handling
        # split datasets when validating the actual content of a dataset.
        # In cases when validating metadata, split datasets don't matter.
        # single dataset. the most common case
        return self.build()

    def get_corresponding_datasets_names(self) -> List[str]:
        directory_path = get_directory_path(self.dataset_path)
        return [
            f"{directory_path}/{dataset['filename']}"
            for dataset in get_corresponding_datasets(self.datasets, self.domain)
        ]

    def get_define_xml_variables_metadata(self) -> List[dict]:
        """
        Gets Define XML variables metadata.
        """
        directory_path = get_directory_path(self.dataset_path)
        define_xml_path: str = f"{directory_path}/{DEFINE_XML_FILE_NAME}"
        define_xml_contents: bytes = self.data_service.get_define_xml_contents(
            dataset_name=define_xml_path
        )
        define_xml_reader = DefineXMLReader.from_file_contents(
            define_xml_contents, cache_service_obj=self.cache
        )
        return define_xml_reader.extract_variables_metadata(domain_name=self.domain)
