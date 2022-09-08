from typing import List, Optional, Callable, TextIO

import pandas as pd

from cdisc_rules_engine.config import ConfigService
from cdisc_rules_engine.dummy_models.dummy_dataset import DummyDataset
from cdisc_rules_engine.exceptions.custom_exceptions import DatasetNotFoundError
from cdisc_rules_engine.interfaces import CacheServiceInterface
from cdisc_rules_engine.models.dataset_types import DatasetTypes
from cdisc_rules_engine.services.data_services import BaseDataService


class DummyDataService(BaseDataService):
    """
    The class returns datasets from provided mock data.
    """

    def __init__(self, data: List[DummyDataset]):
        self.data = data
        super().__init__()

    @classmethod
    def get_instance(
        cls, cache_service: CacheServiceInterface, config: ConfigService, **kwargs
    ):
        data = kwargs.get("data")
        return cls(data)

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
        dataset: Optional[DummyDataset] = self.get_dataset_data(dataset_name)
        metadata_to_return = {}
        if dataset:
            metadata_to_return: dict = dataset.get_metadata()
        return pd.DataFrame.from_dict(metadata_to_return)

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

    def has_all_files(self, prefix: str, file_names: List[str]) -> bool:
        return True

    def join_split_datasets(self, func_to_call: Callable, dataset_names, **kwargs):
        pass

    def read_data(self, file_path: str, read_mode: str = "r") -> TextIO:
        pass
