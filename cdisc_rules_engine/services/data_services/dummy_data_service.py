from datetime import datetime
from io import IOBase
from typing import List, Optional, Callable

import pandas as pd

from cdisc_rules_engine.dummy_models.dummy_dataset import DummyDataset
from cdisc_rules_engine.exceptions.custom_exceptions import DatasetNotFoundError
from cdisc_rules_engine.interfaces import CacheServiceInterface, ConfigInterface
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata
from cdisc_rules_engine.models.dataset_types import DatasetTypes
from cdisc_rules_engine.services.data_readers import DataReaderFactory
from cdisc_rules_engine.services.data_services import BaseDataService


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
        super(DummyDataService, self).__init__(cache_service, reader_factory, config)
        self.data: List[DummyDataset] = kwargs.get("data")

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
        dataset_name = dataset_name.split("/")[-1]
        for dataset in self.data:
            if dataset.filename == dataset_name:
                return dataset
        return None

    def get_dataset(self, dataset_name: str, **params) -> pd.DataFrame:
        dataset: Optional[DummyDataset] = self.get_dataset_data(dataset_name)
        if dataset is not None:
            df: pd.DataFrame = dataset.data
            df = df.applymap(lambda x: x.decode("utf-8") if isinstance(x, bytes) else x)
            self._replace_nans_in_numeric_cols_with_none(df)
            return df
        else:
            return pd.DataFrame.from_dict({})

    def get_dataset_metadata(self, dataset_name: str, **kwargs):
        dataset_metadata: dict = self.__get_dataset_metadata(dataset_name, **kwargs)
        return pd.DataFrame.from_dict(dataset_metadata)

    def get_raw_dataset_metadata(self, dataset_name: str, **kwargs) -> DatasetMetadata:
        dataset_metadata: dict = self.__get_dataset_metadata(dataset_name, **kwargs)
        return DatasetMetadata(
            name=dataset_metadata["dataset_name"][0],
            domain_name=dataset_metadata["dataset_name"][0],
            label=dataset_metadata["dataset_label"][0],
            modification_date=datetime.now().isoformat(),
            filename=dataset_metadata["filename"][0],
            size=dataset_metadata["dataset_size"][0],
            records="",
        )

    def get_variables_metadata(self, dataset_name: str, **params) -> pd.DataFrame:
        metadata_to_return = {
            "variable_name": [],
            "variable_order": [],
            "variable_label": [],
            "variable_size": [],
            "variable_data_type": [],
        }
        dataset: DummyDataset = self.get_dataset_data(dataset_name)
        for i, variable in enumerate(dataset.variables):
            metadata_to_return["variable_name"] = metadata_to_return[
                "variable_name"
            ] + [variable.name]
            metadata_to_return["variable_order"] = metadata_to_return[
                "variable_order"
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

        return pd.DataFrame.from_dict(metadata_to_return)

    def get_dataset_by_type(
        self, dataset_name: str, dataset_type: str, **params
    ) -> pd.DataFrame:
        dataset_type_to_function_map: dict = {
            DatasetTypes.CONTENTS.value: self.get_dataset,
            DatasetTypes.METADATA.value: self.get_dataset_metadata,
            DatasetTypes.VARIABLES_METADATA.value: self.get_variables_metadata,
        }
        return dataset_type_to_function_map[dataset_type](
            dataset_name=dataset_name, **params
        )

    def get_define_xml_contents(self, dataset_name: str) -> bytes:
        pass

    def has_all_files(self, prefix: str, file_names: List[str]) -> bool:
        return True

    def join_split_datasets(self, func_to_call: Callable, dataset_names, **kwargs):
        pass

    def read_data(self, file_path: str) -> IOBase:
        pass

    def __get_dataset_metadata(self, dataset_name: str, **kwargs) -> dict:
        dataset: Optional[DummyDataset] = self.get_dataset_data(dataset_name)
        metadata_to_return = {}
        if dataset:
            metadata_to_return: dict = dataset.get_metadata()
        return metadata_to_return
