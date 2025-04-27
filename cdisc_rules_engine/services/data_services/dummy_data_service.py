from datetime import datetime
from io import IOBase
from json import load
from typing import List, Optional, Iterable, Sequence

import os
import pandas as pd

from cdisc_rules_engine.dummy_models.dummy_dataset import DummyDataset
from cdisc_rules_engine.exceptions.custom_exceptions import DatasetNotFoundError
from cdisc_rules_engine.interfaces import CacheServiceInterface, ConfigInterface
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.dataset_types import DatasetTypes
from cdisc_rules_engine.services.data_readers import DataReaderFactory
from cdisc_rules_engine.services.data_services import BaseDataService
from cdisc_rules_engine.models.dataset import PandasDataset


class DummyDataService(BaseDataService):
    """
    The class returns datasets from provided mock data.
    """

    def __init__(
        self,
        cache_service: CacheServiceInterface,
        reader_factory: DataReaderFactory,
        config: ConfigInterface,
        **kwargs,
    ):
        super(DummyDataService, self).__init__(
            cache_service, reader_factory, config, **kwargs
        )
        self.data: List[DummyDataset] = kwargs.get("data")
        self.define_xml: str = kwargs.get("define_xml")

    @classmethod
    def get_instance(
        cls, cache_service: CacheServiceInterface, config: ConfigInterface, **kwargs
    ):
        return cls(
            cache_service=cache_service,
            reader_factory=DataReaderFactory(),
            config=config,
            **kwargs,
        )

    def check_dataset_exists(self, dataset_name):
        dataset_name = dataset_name.replace("/", "")
        if dataset_name not in self.data:
            raise DatasetNotFoundError("dataset does not exist")

    def get_dataset_data(self, dataset_name: str) -> Optional[DummyDataset]:
        dataset_name = os.path.basename(dataset_name)
        for dataset in self.data:
            if dataset.filename == dataset_name:
                return dataset
        return None

    def get_dataset(self, dataset_name: str, **params) -> PandasDataset:
        dataset: Optional[DummyDataset] = self.get_dataset_data(dataset_name)
        if dataset is not None:
            df: pd.DataFrame = dataset.data
            df = df.applymap(lambda x: x.decode("utf-8") if isinstance(x, bytes) else x)
            result = PandasDataset(df)
            return result
        else:
            return PandasDataset.from_dict({})

    def get_raw_dataset_metadata(
        self, dataset_name: str, **kwargs
    ) -> SDTMDatasetMetadata:
        dataset_metadata: dict = self.__get_dataset_metadata(dataset_name, **kwargs)
        return SDTMDatasetMetadata(
            name=dataset_metadata["dataset_name"][0],
            first_record={"DOMAIN": dataset_metadata["dataset_name"][0]},
            label=dataset_metadata["dataset_label"][0],
            modification_date=datetime.now().isoformat(),
            filename=dataset_metadata["filename"][0],
            file_size=dataset_metadata["dataset_size"][0],
            full_path=dataset_metadata["filename"][0],
            record_count=dataset_metadata["record_count"][0],
        )

    def get_variables_metadata(self, dataset_name: str, **params) -> PandasDataset:
        metadata_to_return = {
            "variable_name": [],
            "variable_order_number": [],
            "variable_label": [],
            "variable_size": [],
            "variable_data_type": [],
            "variable_format": [],
        }
        dataset: DummyDataset = self.get_dataset_data(dataset_name)
        for i, variable in enumerate(dataset.variables):
            metadata_to_return["variable_name"] = metadata_to_return[
                "variable_name"
            ] + [variable.name]
            metadata_to_return["variable_order_number"] = metadata_to_return[
                "variable_order_number"
            ] + [i + 1]
            metadata_to_return["variable_label"] = metadata_to_return[
                "variable_label"
            ] + [variable.label]
            metadata_to_return["variable_size"] = metadata_to_return[
                "variable_size"
            ] + [variable.length]
            metadata_to_return["variable_data_type"] = metadata_to_return[
                "variable_data_type"
            ] + [variable.type]
            metadata_to_return["variable_format"] = metadata_to_return[
                "variable_format"
            ] + [variable.format]
        return PandasDataset.from_dict(metadata_to_return)

    def get_dataset_by_type(
        self, dataset_name: str, dataset_type: str, **params
    ) -> PandasDataset:
        dataset_type_to_function_map: dict = {
            DatasetTypes.CONTENTS.value: self.get_dataset,
            DatasetTypes.METADATA.value: self.get_dataset_metadata,
            DatasetTypes.VARIABLES_METADATA.value: self.get_variables_metadata,
        }
        return dataset_type_to_function_map[dataset_type](
            dataset_name=dataset_name, **params
        )

    def get_define_xml_contents(self, dataset_name: str) -> bytes:
        if not self.define_xml:
            # Search for define xml locally
            with open(dataset_name, "rb") as f:
                return f.read()

        return self.define_xml.encode()

    def has_all_files(self, prefix: str, file_names: List[str]) -> bool:
        return True

    def get_file_matching_pattern(self, prefix: str, pattern: str) -> str:
        """
        Returns the path to the file if one matches the pattern given, otherwise
        return None.
        """
        return None

    def read_data(self, file_path: str) -> IOBase:
        return open(file_path, "rb")

    def __get_dataset_metadata(self, dataset_name: str, **kwargs) -> dict:
        dataset: Optional[DummyDataset] = self.get_dataset_data(dataset_name)
        metadata_to_return = {}
        if dataset:
            metadata_to_return: dict = dataset.get_metadata()
        return metadata_to_return

    def to_parquet(self, file_path: str) -> str:
        return ""

    def get_datasets(self) -> Iterable[SDTMDatasetMetadata]:
        return self.data

    @staticmethod
    def get_data(dataset_paths: Sequence[str]):
        with open(dataset_paths[0]) as fp:
            json = load(fp)
            return [DummyDataset(data) for data in json.get("datasets", [])]

    @staticmethod
    def is_valid_data(dataset_paths: Sequence[str]):
        if (
            dataset_paths
            and len(dataset_paths) == 1
            and dataset_paths[0].lower().endswith(".json")
        ):
            with open(dataset_paths[0]) as fp:
                json = load(fp)
                return "datasets" in json
        return False
