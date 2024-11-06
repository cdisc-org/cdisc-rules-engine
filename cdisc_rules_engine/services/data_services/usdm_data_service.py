import os
from io import IOBase
from typing import Iterable, List
from json import load
from jsonpath_ng import DatumInContext
from jsonpath_ng.ext import parse
from datetime import datetime
from yaml import safe_load
from numpy import empty, vectorize
import re

from cdisc_rules_engine.interfaces import CacheServiceInterface, ConfigInterface
from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata
from cdisc_rules_engine.models.dataset_types import DatasetTypes
from cdisc_rules_engine.models.variable_metadata_container import (
    VariableMetadataContainer,
)
from cdisc_rules_engine.services.data_readers.data_reader_factory import (
    DataReaderFactory,
)
from cdisc_rules_engine.utilities.utils import (
    extract_file_name_from_path_string,
)
from .base_data_service import BaseDataService, cached_dataset


class USDMDataService(BaseDataService):
    _instance = None

    def __init__(
        self,
        cache_service: CacheServiceInterface,
        reader_factory: DataReaderFactory,
        config: ConfigInterface,
        **kwargs,
    ):
        super(USDMDataService, self).__init__(
            cache_service, reader_factory, config, **kwargs
        )
        self.dataset_path: str = kwargs.get("dataset_path", "")

        with open(os.path.join("resources", "schema", "USDM.yaml")) as entity_dict:
            self.entity_dict: dict = safe_load(entity_dict)

        self.json = self._reader_factory.get_service("USDM").from_file(
            self.dataset_path
        )

        self.dataset_content_index: dict = self.__get_datasets_content_index(
            dataset_name="USDM_content_index", json=self.json
        )

    @classmethod
    def get_instance(
        cls,
        cache_service: CacheServiceInterface,
        config: ConfigInterface = None,
        **kwargs,
    ):
        if cls._instance is None:
            service = cls(
                cache_service=cache_service,
                reader_factory=DataReaderFactory(
                    dataset_implementation=kwargs.get(
                        "dataset_implementation", PandasDataset
                    )
                ),
                config=config,
                **kwargs,
            )
            cls._instance = service
        return cls._instance

    def has_all_files(self, prefix: str, file_names: List[str]) -> bool:
        return os.path.isfile(self.dataset_path)

    def get_file_matching_pattern(self, prefix: str, pattern: str) -> str:
        """
        Returns the path to the file if one matches the pattern given, otherwise
        return None.
        """
        for f in os.listdir(prefix):
            if os.path.isfile(os.path.join(prefix, f)) and re.match(pattern, f):
                return f
        return None

    @cached_dataset(DatasetTypes.CONTENTS.value)
    def get_dataset(self, dataset_name: str, **params) -> DatasetInterface:
        return self.__get_dataset(dataset_name)

    @cached_dataset(DatasetTypes.METADATA.value)
    def get_dataset_metadata(
        self, dataset_name: str, size_unit: str = None, **params
    ) -> DatasetInterface:
        """
        Gets metadata of a dataset and returns it as a DataFrame.
        """
        metadata_to_return: dict = {
            "dataset_name": [dataset_name],
        }
        return self._reader_factory.dataset_implementation.from_dict(metadata_to_return)

    @cached_dataset(DatasetTypes.RAW_METADATA.value)
    def get_raw_dataset_metadata(self, dataset_name: str, **kwargs) -> DatasetMetadata:
        """
        Returns dataset metadata as DatasetMetadata instance.
        """
        dataset = self.get_dataset(dataset_name=dataset_name)
        domain_name = self.__get_domain_from_dataset_name(dataset_name)
        return DatasetMetadata(
            name=dataset_name,
            domain_name=domain_name,
            label=domain_name,
            modification_date=datetime.fromtimestamp(
                os.path.getmtime(self.dataset_path)
            ).isoformat(),
            filename=extract_file_name_from_path_string(dataset_name),
            full_path=dataset_name,
            size=0,
            records=len(dataset),
        )

    @cached_dataset(DatasetTypes.VARIABLES_METADATA.value)
    def get_variables_metadata(self, dataset_name: str, **params) -> DatasetInterface:
        """
        Gets dataset from blob storage and returns metadata of a certain variable.
        """
        metadata: dict = self.read_metadata(dataset_name)
        contents_metadata: dict = metadata["contents_metadata"]
        metadata_to_return: VariableMetadataContainer = VariableMetadataContainer(
            contents_metadata
        )
        return self._reader_factory.dataset_implementation.from_dict(
            metadata_to_return.to_representation()
        )

    @cached_dataset(DatasetTypes.CONTENTS.value)
    def get_define_xml_contents(self, dataset_name: str) -> bytes:
        """
        Reads local define xml file as bytes
        """
        raise NotImplementedError(
            "Can't use 'get_define_xml_contents' in USDMDataService!"
        )

    def read_metadata(self, dataset_name: str) -> dict:
        np_json_type_map: dict = {"O": "string", "float64": "float"}
        file_size = os.path.getsize(self.dataset_path)
        file_name = extract_file_name_from_path_string(self.dataset_path)
        file_metadata = {
            "path": self.dataset_path,
            "name": file_name,
            "size": file_size,
        }
        dataset = self.__get_dataset(dataset_name)
        measurer = vectorize(len)
        contents_metadata = {
            "variable_labels": dataset.columns.values.tolist(),
            "variable_names": dataset.columns.values.tolist(),
            "variable_formats": empty(dataset.data.shape[1], dtype=str).tolist(),
            "variable_name_to_label_map": dict(zip(dataset.data, dataset.data)),
            "variable_name_to_data_type_map": dict(
                zip(dataset.data, dataset.data.dtypes.map(np_json_type_map))
            ),
            "variable_name_to_size_map": dict(
                zip(dataset.data, measurer(dataset.data.values.astype(str)).max(axis=0))
            ),
            "number_of_variables": len(dataset.columns.values.tolist()),
            "dataset_label": dataset_name,
            "dataset_length": len(dataset),
            "domain_name": dataset_name,
            "dataset_name": dataset_name,
            "dataset_modification_date": datetime.fromtimestamp(
                os.path.getmtime(self.dataset_path)
            ).isoformat(),
        }

        return {
            "file_metadata": file_metadata,
            "contents_metadata": contents_metadata,
        }

    def read_data(self, file_path: str) -> IOBase:
        return open(file_path, "rb")

    def get_datasets(self) -> List[dict]:
        datasets = []
        for dataset in self.dataset_content_index:
            metadata = self.get_raw_dataset_metadata(
                dataset_name=dataset["dataset_name"]
            )
            datasets.append(
                {
                    "domain": metadata.domain_name,
                    "filename": metadata.filename,
                    "full_path": metadata.full_path,
                    "length": metadata.records,
                    "label": metadata.label,
                    "size": metadata.size,
                    "modification_date": metadata.modification_date,
                }
            )
        return datasets

    def to_parquet(self, file_path: str) -> str:
        """
        Stub implementation to satisfy abstract interface requirements.

        This method exists only to fulfill the abstract method requirement from the parent class.
        While implemented to prevent TypeError, it is not intended to be called in this class.
        Other classes implementing this interface make actual use of to_parquet().
        """
        raise NotImplementedError("to_parquet is not supported for this class")

    def __get_record_data(self, node: dict, parent="") -> dict:
        if type(node) is dict:
            flattened = {}
            for key, value in node.items():
                flattened[f"{parent}{key}"] = (
                    (len(value) > 0) if type(value) in (dict, list) else value
                )
                if type(value) is dict:
                    flattened |= self.__get_record_data(value, f"{parent}{key}.")
        else:
            flattened = {"value": node}
        return flattened

    @staticmethod
    def __get_parent(node) -> DatumInContext:
        return (
            node.context if node.context and type(node.context.value) is list else node
        )

    @staticmethod
    def __get_closest_non_list_ancestor(node) -> DatumInContext:
        return (
            node.context.context if type(node.context.value) is list else node.context
        )

    def __get_record_metadata(self, node) -> dict:
        closest_non_list_ancestor = USDMDataService.__get_closest_non_list_ancestor(
            node
        )
        record = {
            "parent_entity": self.__get_entity_name(
                closest_non_list_ancestor.value,
                USDMDataService.__get_parent(closest_non_list_ancestor),
            ),
            "parent_id": closest_non_list_ancestor.value.get("id", ""),
            "parent_rel": f"{USDMDataService.__get_parent(node).path}",
            "rel_type": node.type,
        }
        return record

    def __get_dataset(self, dataset_name: str) -> DatasetInterface:
        datasets = self.dataset_content_index
        dataset_paths = [
            path
            for dataset_metadata in datasets
            if dataset_metadata["dataset_name"] == dataset_name
            for path in dataset_metadata["content_paths"]
        ]
        all_nodes = []
        for dataset_path in dataset_paths:
            nodes = [match for match in parse(dataset_path["path"]).find(self.json)]
            if len(nodes) != 1:
                raise Exception(
                    f"Multiple objects found with path: {dataset_path['path']}"
                )
            node = nodes[0]
            if dataset_path["type"] == "reference":
                node.value = self.__find_definition(self.json, node.value)
                node.type = "reference"
            else:
                node.type = "definition"
            all_nodes.append(node)
        records = [
            self.__get_record_metadata(node) | self.__get_record_data(node.value)
            for node in all_nodes
        ]
        return self._reader_factory.dataset_implementation.from_records(records)

    def __find_definition(self, json, id: str):
        definition = parse(f"$..*[?(@.id = '{id}')]").find(json)
        if definition:
            if len(definition) > 1:
                raise Exception(f"Multiple objects found with id: {id}")
            return definition[0].value
        return None

    def __get_entity_name(self, value, parent: DatumInContext):
        if type(value) is dict:
            api_type = (
                value.get("instanceType")
                if "instanceType" in value
                else f"{parent.path}"
            )
        else:
            # primitive types
            api_type = value.__class__.__name__
        mapped_entity = self.entity_dict.get(api_type, api_type)
        if isinstance(mapped_entity, str):
            return mapped_entity
        else:
            closest_non_list_ancestor = USDMDataService.__get_closest_non_list_ancestor(
                parent
            )
            return mapped_entity.get(
                self.__get_entity_name(
                    closest_non_list_ancestor.value,
                    USDMDataService.__get_parent(closest_non_list_ancestor),
                ),
                api_type,
            )

    def __read_metadata(
        self,
        json,
        parent_node: DatumInContext,
        child_value,
        content_path: str,
    ):
        ty = "definition"
        if type(child_value) is str and (
            f"{parent_node.path}".endswith("Id")
            or f"{parent_node.path}".endswith("Ids")
        ):
            definition = self.__find_definition(json, child_value)
            if definition:
                child_value = definition
                ty = "reference"
        entity_name = self.__get_entity_name(child_value, parent_node)
        return {
            "path": content_path,
            "entity": entity_name,
            "type": ty,
        }

    @staticmethod
    def __get_full_path(node: DatumInContext):
        return f"{node.full_path}".replace(".[", "[")

    @cached_dataset(DatasetTypes.CONTENTS.value)
    def __get_datasets_content_index(self, dataset_name: str, json) -> List[dict]:
        """
        This is a bit convoluted because there is a bug in jsonpath_ng
        where this query does not return object values within an array
        """
        metadata = []
        for node in parse("$..*").find(json):
            if type(node.value) is list:
                for index, child in enumerate(node.value):
                    if metadatum := self.__read_metadata(
                        json,
                        node,
                        child,
                        f"{USDMDataService.__get_full_path(node)}[{index}]",
                    ):
                        metadata.append(metadatum)
            else:
                if metadatum := self.__read_metadata(
                    json, node, node.value, USDMDataService.__get_full_path(node)
                ):
                    metadata.append(metadatum)
        dataset_dict = {}
        for path in metadata:
            dataset_dict.setdefault(path["entity"], []).append(
                {"path": path["path"], "type": path["type"]}
            )
        return [
            {
                "dataset_name": self.__get_dataset_name_from_domain(key),
                "domain": key,
                "content_paths": value,
            }
            for key, value in dataset_dict.items()
        ]

    def __get_dataset_name_from_domain(self, domain_name: str) -> str:
        return os.path.join(self.dataset_path, "{}.json".format(domain_name))

    def __get_domain_from_dataset_name(self, dataset_name: str) -> str:
        return extract_file_name_from_path_string(dataset_name).split(".")[0]

    @staticmethod
    def is_USDM_data(dataset_paths: Iterable[str]):
        if (
            dataset_paths
            and len(dataset_paths) == 1
            and dataset_paths[0].lower().endswith(".json")
        ):
            with open(dataset_paths[0]) as fp:
                json = load(fp)
                return "study" in json and "datasetJSONVersion" not in json
        return False
