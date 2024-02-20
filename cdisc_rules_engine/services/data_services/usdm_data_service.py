import os
from io import IOBase
from typing import Iterable, List, Union
from json import load
import pandas
from jsonpath_ng import DatumInContext, Fields, This
from jsonpath_ng.ext import parse
from datetime import datetime
from yaml import safe_load
from numpy import empty, vectorize

from cdisc_rules_engine.interfaces import CacheServiceInterface, ConfigInterface
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
                reader_factory=DataReaderFactory(),
                config=config,
                **kwargs,
            )
            cls._instance = service
        return cls._instance

    def has_all_files(self, prefix: str, file_names: List[str]) -> bool:
        return os.path.isfile(self.dataset_path)

    @cached_dataset(DatasetTypes.CONTENTS.value)
    def get_dataset(self, dataset_name: str, **params) -> pandas.DataFrame:
        return self.__get_dataset(dataset_name)

    @cached_dataset(DatasetTypes.METADATA.value)
    def get_dataset_metadata(
        self, dataset_name: str, size_unit: str = None, **params
    ) -> pandas.DataFrame:
        """
        Gets metadata of a dataset and returns it as a DataFrame.
        """
        metadata_to_return: dict = {
            "dataset_name": [dataset_name],
        }
        return pandas.DataFrame.from_dict(metadata_to_return)

    @cached_dataset(DatasetTypes.RAW_METADATA.value)
    def get_raw_dataset_metadata(self, dataset_name: str, **kwargs) -> DatasetMetadata:
        """
        Returns dataset metadata as DatasetMetadata instance.
        """
        dataset = self.__get_dataset(dataset_name)
        return DatasetMetadata(
            name=dataset_name,
            domain_name=dataset_name,
            label="",
            modification_date=datetime.fromtimestamp(
                os.path.getmtime(self.dataset_path)
            ).isoformat(),
            filename=os.path.basename(self.dataset_path),
            full_path=self.dataset_path,
            size=0,
            records=f"{dataset.shape[0]}",
        )

    @cached_dataset(DatasetTypes.VARIABLES_METADATA.value)
    def get_variables_metadata(self, dataset_name: str, **params) -> pandas.DataFrame:
        """
        Gets dataset from blob storage and returns metadata of a certain variable.
        """
        metadata: dict = self.read_metadata(dataset_name)
        contents_metadata: dict = metadata["contents_metadata"]
        metadata_to_return: VariableMetadataContainer = VariableMetadataContainer(
            contents_metadata
        )
        return pandas.DataFrame.from_dict(metadata_to_return.to_representation())

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
            "variable_formats": empty(dataset.shape[1], dtype=str).tolist(),
            "variable_name_to_label_map": dict(zip(dataset, dataset)),
            "variable_name_to_data_type_map": dict(
                zip(dataset, dataset.dtypes.map(np_json_type_map))
            ),
            "variable_name_to_size_map": dict(
                zip(dataset, measurer(dataset.values.astype(str)).max(axis=0))
            ),
            "number_of_variables": dataset.shape[1],
            "dataset_label": dataset_name,
            "dataset_length": dataset.shape[0],
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
        json = self._reader_factory.get_service("USDM").from_file(self.dataset_path)
        return self.__get_datasets(json)

    def __get_record_data(self, node: dict, parent="") -> dict:
        if type(node) is dict:
            flattened = {}
            for key, value in node.items():
                if type(value) is dict:
                    flattened |= self.__get_record_data(value, f"{parent}{key}.")
                elif type(value) is not list:
                    flattened[f"{parent}{key}"] = value
        else:
            flattened = {"value": node}
        return flattened

    @staticmethod
    def __get_parent_rel(node) -> Union[Fields, This]:
        return (
            node.context.path
            if node.context and type(node.context.value) is list
            else node.path
        )

    def __get_record_metadata(self, node) -> dict:
        closest_non_list_ancestor = (
            node.context.context if type(node.context.value) is list else node.context
        )
        record = {
            "parent_entity": self.__get_entity_name(
                closest_non_list_ancestor.value,
                USDMDataService.__get_parent_rel(closest_non_list_ancestor),
            ),
            "parent_id": closest_non_list_ancestor.value.get("id", ""),
            "parent_rel": f"{USDMDataService.__get_parent_rel(node)}",
            "rel_type": node.type,
        }
        return record

    def __get_dataset(self, dataset_name: str) -> pandas.DataFrame:
        json = self._reader_factory.get_service("USDM").from_file(self.dataset_path)
        datasets = self.__get_datasets(json)
        dataset_paths = [
            path
            for dataset_metadata in datasets
            if dataset_metadata["domain"] == dataset_name
            for path in dataset_metadata["full_path"]
        ]
        all_nodes = []
        for dataset_path in dataset_paths:
            nodes = [match for match in parse(dataset_path["path"]).find(json)]
            if len(nodes) != 1:
                raise Exception(
                    f"Multiple objects found with path: {dataset_path['path']}"
                )
            node = nodes[0]
            if dataset_path["type"] == "reference":
                node.value = self.__find_definition(json, node.value)
                node.type = "reference"
            else:
                node.type = "definition"
            all_nodes.append(node)
        records = [
            self.__get_record_metadata(node) | self.__get_record_data(node.value)
            for node in all_nodes
        ]
        return pandas.DataFrame(records)

    def __find_definition(self, json, id: str):
        definition = parse(f"$..*[?(@.id = '{id}')]").find(json)
        if definition:
            if len(definition) > 1:
                raise Exception(f"Multiple objects found with id: {id}")
            return definition[0].value
        return None

    def __get_entity_name(self, value, path: Union[Fields, This]):
        if type(value) is dict:
            api_type = (
                value.get("instanceType") if "instanceType" in value else f"{path}"
            )
        else:
            # primitive types
            api_type = value.__class__.__name__
        return self.entity_dict.get(api_type, api_type)

    def __read_metadata(
        self,
        json,
        parent_node: DatumInContext,
        child_value,
        full_path: str,
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
        entity_name = self.__get_entity_name(child_value, parent_node.path)
        return {
            "path": full_path,
            "entity": entity_name,
            "type": ty,
        }

    @staticmethod
    def __get_full_path(node: DatumInContext):
        return f"{node.full_path}".replace(".[", "[")

    def __get_datasets(self, json) -> List[dict]:
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
            {"domain": key, "full_path": value} for key, value in dataset_dict.items()
        ]

    @staticmethod
    def is_USDM_data(dataset_paths: Iterable[str]):
        if len(dataset_paths) == 1 and dataset_paths[0].lower().endswith(".json"):
            with open(dataset_paths[0]) as fp:
                json = load(fp)
                return "study" in json and "datasetJSONVersion" not in json
        return False
