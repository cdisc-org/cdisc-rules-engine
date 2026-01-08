import os
from io import IOBase
from typing import List, Sequence, Any
from dataclasses import dataclass
from jsonpath_ng import DatumInContext
from jsonpath_ng.ext import parse
from datetime import datetime
from yaml import safe_load
from numpy import empty, vectorize
import re

from cdisc_rules_engine.interfaces import CacheServiceInterface, ConfigInterface
from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.dataset_types import DatasetTypes
from cdisc_rules_engine.models.variable_metadata_container import (
    VariableMetadataContainer,
)

from cdisc_rules_engine.services.data_readers.data_reader_factory import (
    DataReaderFactory,
)
from cdisc_rules_engine.services.data_readers.json_reader import JSONReader
from cdisc_rules_engine.utilities.utils import (
    extract_file_name_from_path_string,
)
from .base_data_service import BaseDataService, cached_dataset


# Node dataclass for dataset traversal, now with parent tracking
@dataclass
class Node:
    value: Any
    path: str
    type: str
    parent: dict = None


class USDMDataService(BaseDataService):
    # Pre-compiled regex pattern for matching list indices
    _LIST_INDEX_REGEX = re.compile(r"([\w-]+)(\[(\d+)\])?")

    def _traverse_path(self, obj, path):
        """
        Traverse a nested dict/list using a path string like 'foo.bar[0].baz'.
        Returns the node at the path, or raises KeyError/IndexError if not found.
        Supports dot notation and list indices.
        """
        parts = re.split(r"\.(?![^\[]*\])", path)  # split on dot not inside brackets
        current = obj
        for part in parts:
            # Handle list index, e.g. 'foo[3]'
            m = self._LIST_INDEX_REGEX.match(part)
            if not m:
                raise KeyError(f"Invalid path segment: {part}")
            key = m.group(1)
            idx = m.group(3)
            if isinstance(current, dict):
                current = current[key]
            else:
                raise KeyError(f"Expected dict at {key} in {part}")
            if idx is not None:
                current = current[int(idx)]
        return current

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

        # Build the id lookup dict once for fast reference resolution
        self._id_lookup = self.__build_id_lookup(self.json)

        self.dataset_content_index: dict = self.__get_datasets_content_index(
            dataset_name="USDM_content_index", json=self.json
        )

        self._jsonpath_cache = {}

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

    @cached_dataset(DatasetTypes.RAW_METADATA.value)
    def get_raw_dataset_metadata(
        self, dataset_name: str, **kwargs
    ) -> SDTMDatasetMetadata:
        """
        Returns dataset metadata as DatasetMetadata instance.
        """
        dataset = self.get_dataset(dataset_name=dataset_name)
        domain = self.__get_domain_from_dataset_name(dataset_name)
        return SDTMDatasetMetadata(
            name=domain,
            first_record={"DOMAIN": domain},
            label=domain,
            modification_date=datetime.fromtimestamp(
                os.path.getmtime(self.dataset_path)
            ).isoformat(),
            filename=extract_file_name_from_path_string(dataset_name),
            full_path=dataset_name,
            file_size=0,
            record_count=len(dataset),
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
            "file_size": file_size,
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
            "domain": dataset_name,
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
            dataset_name = dataset.get("dataset_name")
            if not dataset_name:
                continue
            dataset_metadata: SDTMDatasetMetadata = self.get_raw_dataset_metadata(
                dataset_name=dataset_name
            )
            datasets.append(dataset_metadata)
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
    def __get_parent(node):
        # Native node: just return node itself
        return node

    @staticmethod
    def __get_closest_non_list_ancestor(node):
        # Native node: just return node itself
        return node

    @staticmethod
    def jsonpath_to_pointer(path_expr: str) -> str:
        pointer = path_expr.replace("$.", "/").replace(".", "/")
        pointer = re.sub(r"\[(\d+)\]", r"/\1", pointer)
        return f"/{pointer}"

    def __get_record_metadata(self, node: Node) -> dict:
        # Walk up the parent chain to find the closest ancestor with instanceType and id
        parent = getattr(node, "parent", None)
        parent_entity = ""
        parent_id = ""
        while parent and isinstance(parent, dict):
            if parent.get("instanceType"):
                # Use schema mapping for instanceType if available
                parent_entity = self.entity_dict.get(
                    parent["instanceType"], parent["instanceType"]
                )
                parent_id = parent.get("id", "")
                break
            parent = (
                parent.get("parent") if isinstance(parent.get("parent"), dict) else None
            )
        path = getattr(node, "path", "")
        # Remove trailing [index] if present
        path_no_index = re.sub(r"\[\d+\]$", "", path)
        # Get the last attribute after splitting by '.'
        parent_rel = (
            path_no_index.split(".")[-1] if "." in path_no_index else path_no_index
        )
        rel_type = getattr(node, "type", "")
        # 'Wrapper' for all top-level Study attributes (any path starting with '`this`' or with no dot)
        if path.startswith("`this`") or "." not in path:
            # parent_entity = self.entity_dict["`this`"]
            parent_entity = self.entity_dict.get("`this`")
        # If still not set, use schema mapping for node type or key
        if not parent_entity:
            node_type = getattr(node, "type", None)
            if node_type and node_type in self.entity_dict:
                parent_entity = self.entity_dict.get(node_type)
            elif path:
                key = path.split(".")[-1] if "." in path else path
                parent_entity = self.entity_dict.get(key, key)

        record = {
            "parent_entity": parent_entity,
            "parent_id": parent_id,
            "parent_rel": parent_rel,
            "rel_type": rel_type,
            "_path": self.jsonpath_to_pointer(path),
        }
        return record

    def __build_id_lookup(self, obj=None):
        """Iteratively build a dict mapping id -> object."""
        lookup = {}
        stack = [obj if obj is not None else self.json]
        while stack:
            current = stack.pop()
            if isinstance(current, dict):
                if "id" in current:
                    lookup[current["id"]] = current
                stack.extend(current.values())
            elif isinstance(current, list):
                stack.extend(current)
        return lookup

    def __find_definition(self, json, id: str):
        # Use the pre-built lookup dict for fast access
        return self._id_lookup.get(id, None)

    def _get_parsed_jsonpath(self, path_expr):
        key = path_expr.strip()
        if key not in self._jsonpath_cache:
            self._jsonpath_cache[key] = parse(key)
        return self._jsonpath_cache[key]

    def __get_dataset(self, dataset_name: str) -> DatasetInterface:
        datasets = self.dataset_content_index
        dataset_paths = [
            path
            for dataset_metadata in datasets
            if dataset_metadata["dataset_name"] == dataset_name
            for path in dataset_metadata["content_paths"]
        ]
        all_nodes = []

        def get_parent_from_path(obj, path):
            # Traverse to the parent of the node at the given path
            if not path or "." not in path:
                return None
            parent_path = ".".join(path.split(".")[:-1])
            # print("DEBUG:", parent_path)
            try:
                return self._traverse_path(obj, parent_path)
            except Exception:
                return None

        for dataset_path in dataset_paths:
            try:
                value = self._traverse_path(self.json, dataset_path["path"])
            except (KeyError, IndexError) as e:
                raise Exception(f"Path not found: {dataset_path['path']} ({e})")
            if dataset_path["type"] == "reference":
                node_value = self.__find_definition(self.json, value)
                node_type = "reference"
            else:
                node_value = value
                node_type = "definition"
            parent = get_parent_from_path(self.json, dataset_path["path"])
            node = Node(
                value=node_value,
                path=dataset_path["path"],
                type=node_type,
                parent=parent,
            )
            all_nodes.append(node)
        records = [
            self.__get_record_metadata(node) | self.__get_record_data(node.value)
            for node in all_nodes
        ]
        return self._reader_factory.dataset_implementation.from_records(records)

    def __get_entity_name(self, value, parent: Any, _depth=0):
        # Recursion guard to prevent infinite recursion
        if _depth > 25:
            return "UnknownEntity"
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
            return api_type

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
            entity = path["entity"]
            if entity.lower() == "code":
                entity = "Code"
            # Do not skip 'null' entities; include them as datasets
            dataset_dict.setdefault(entity, []).append(
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
    def is_valid_data(dataset_paths: Sequence[str]):
        if (
            dataset_paths
            and len(dataset_paths) == 1
            and dataset_paths[0].lower().endswith(".json")
        ):
            json = JSONReader().from_file(dataset_paths[0])
            return "study" in json and "datasetJSONVersion" not in json
        return False
