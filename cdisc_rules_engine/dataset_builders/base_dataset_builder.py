from abc import abstractmethod
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)
from cdisc_rules_engine.utilities.utils import (
    get_corresponding_datasets,
    tag_source,
)
from typing import List, Iterable
from cdisc_rules_engine.utilities import sdtm_utilities
from cdisc_rules_engine.utilities.rule_processor import RuleProcessor
from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.interfaces.data_service_interface import DataServiceInterface


class BaseDatasetBuilder:
    def __init__(
        self,
        rule,
        data_service: DataServiceInterface,
        cache_service,
        rule_processor: RuleProcessor,
        data_processor,
        dataset_path,
        datasets: Iterable[SDTMDatasetMetadata],
        dataset_metadata: SDTMDatasetMetadata,
        define_xml_path,
        standard,
        standard_version,
        standard_substandard,
        library_metadata=LibraryMetadataContainer(),
    ):
        self.data_service = data_service
        self.cache = cache_service
        self.data_processor = data_processor
        self.rule_processor = rule_processor
        self.dataset_path = dataset_path
        self.datasets = datasets
        self.dataset_metadata = dataset_metadata
        self.rule = rule
        self.define_xml_path = define_xml_path
        self.standard = standard
        self.standard_version = standard_version
        self.standard_substandard = standard_substandard
        self.library_metadata = library_metadata
        self.dataset_implementation = self.data_service.dataset_implementation

    @abstractmethod
    def build(self) -> DatasetInterface:
        """
        Returns correct dataframe to operate on
        """
        pass

    def build_split_datasets(self, dataset_name, **kwargs) -> DatasetInterface:
        """
        Returns correct dataframe to operate on.
        Default implementation that temporarily sets dataset_path to dataset_name and calls build().
        """
        original_path = self.dataset_path
        try:
            self.dataset_path = dataset_name
            result = self.build(**kwargs)
            return result
        finally:
            self.dataset_path = original_path

    def get_dataset(self, **kwargs):
        # If validating dataset content, ensure split datasets are handled.
        if self.dataset_metadata.is_split:
            # Handle split datasets for content checks.
            # A content check is any check that is not in the list of rule types
            dataset: DatasetInterface = self.data_service.concat_split_datasets(
                func_to_call=self.build_split_datasets,
                datasets_metadata=get_corresponding_datasets(
                    self.datasets, self.dataset_metadata
                ),
                **kwargs,
            )
        else:
            # single dataset. the most common case
            dataset: DatasetInterface = self.build()
            dataset = tag_source(dataset, self.dataset_metadata)
        return dataset

    def get_dataset_contents(self, **kwargs):
        # If validating dataset content, ensure split datasets are handled.
        if self.dataset_metadata.is_split:
            # Handle split datasets for content checks.
            # A content check is any check that is not in the list of rule types
            dataset: DatasetInterface = self.data_service.concat_split_datasets(
                func_to_call=self.data_service.get_dataset,
                datasets_metadata=get_corresponding_datasets(
                    self.datasets, self.dataset_metadata
                ),
                **kwargs,
            )
        else:
            # single dataset. the most common case
            dataset: DatasetInterface = self.data_service.get_dataset(self.dataset_path)
            dataset = tag_source(dataset, self.dataset_metadata)
        return dataset

    def get_define_xml_item_group_metadata_for_dataset(
        self, dataset_metadata: SDTMDatasetMetadata
    ) -> List[dict]:
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
            "define_dataset_key_sequence"
        """

        define_xml_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.dataset_path, self.define_xml_path, self.data_service, self.cache
        )
        return define_xml_reader.extract_dataset_metadata(
            dataset_metadata["dataset_name"]
        )

    def get_define_xml_item_group_metadata_for_domain(self, domain: str) -> List[dict]:
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
            "define_dataset_key_sequence"
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
        return define_xml_reader.extract_variables_metadata(
            domain_name=self.dataset_metadata.domain
        )

    def get_define_xml_value_level_metadata(self) -> List[dict]:
        """
        Gets Define XML value level metadata and returns it as dataframe.
        """
        define_xml_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.dataset_path, self.define_xml_path, self.data_service, self.cache
        )
        return define_xml_reader.extract_value_level_metadata(
            domain_name=self.dataset_metadata.domain
        )

    @staticmethod
    def add_row_number(dataframe: DatasetInterface) -> None:
        dataframe["row_number"] = list(range(1, len(dataframe.data) + 1))

    def get_define_metadata(self):
        define_xml_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.dataset_path, self.define_xml_path, self.data_service, self.cache
        )
        return define_xml_reader.read()

    def get_library_variables_metadata(self) -> DatasetInterface:
        # TODO: Update to support other standard types
        if self.datasets:
            dataset = self.get_dataset_contents()
            dataset_class = self.data_service.get_dataset_class(
                dataset,
                self.dataset_path,
                self.datasets,
                getattr(self, "domain", self.dataset_metadata),
            )
        else:
            dataset_class = None
        variables: List[dict] = sdtm_utilities.get_variables_metadata_from_standard(
            domain=self.dataset_metadata.domain,
            library_metadata=self.library_metadata,
            dataset_class=dataset_class,
        )

        # Rename columns:
        column_name_mapping = {
            "ordinal": "order_number",
            "simpleDatatype": "data_type",
        }

        for var in variables:
            var["name"] = var["name"].replace("--", self.dataset_metadata.domain)
            for key, new_key in column_name_mapping.items():
                if key in var:
                    var[new_key] = var.pop(key)

        dataset = self.dataset_implementation.from_records(variables)
        dataset.data = dataset.data.add_prefix("library_variable_")
        return dataset
