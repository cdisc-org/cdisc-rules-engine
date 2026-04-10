from datetime import datetime
from io import IOBase
from typing import List, Optional, Sequence

import pandas as pd
import tempfile
from cdisc_rules_engine.dummy_models.dummy_dataset import DummyDataset
from cdisc_rules_engine.interfaces import CacheServiceInterface, ConfigInterface
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.dataset_types import DatasetTypes
from cdisc_rules_engine.services.data_readers import DataReaderFactory
from cdisc_rules_engine.services.data_readers.json_reader import JSONReader
from cdisc_rules_engine.services.data_services import BaseDataService
from cdisc_rules_engine.constants import DEFAULT_ENCODING
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
        self.data: List[DummyDataset] = kwargs.get("data")
        self.define_xml: str = kwargs.get("define_xml")
        super(DummyDataService, self).__init__(
            cache_service, reader_factory, config, **kwargs
        )

    @classmethod
    def get_instance(
        cls, cache_service: CacheServiceInterface, config: ConfigInterface, **kwargs
    ):
        return cls(
            cache_service=cache_service,
            reader_factory=DataReaderFactory(
                dataset_implementation=kwargs.get(
                    "dataset_implementation", PandasDataset
                ),
                encoding=kwargs.get("encoding"),
            ),
            config=config,
            **kwargs,
        )

    def get_dataset_data(self, dataset_name: str) -> Optional[DummyDataset]:
        for dataset in self.data:
            if dataset.name == dataset_name:
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

    def _initialize_datasets_metadata(self, **kwargs) -> dict[str, SDTMDatasetMetadata]:
        """
        Initialize the dataset metadata by converting DummyDataset objects to SDTMDatasetMetadata.

        Returns:
            Dictionary mapping dataset name to SDTMDatasetMetadata
        """
        result = {}
        for dataset in self.data:
            dataset_metadata_dict: dict = dataset.get_metadata()
            metadata = SDTMDatasetMetadata(
                name=dataset_metadata_dict["dataset_name"][0],
                first_record={"DOMAIN": dataset_metadata_dict["dataset_name"][0]},
                label=dataset_metadata_dict["dataset_label"][0],
                modification_date=datetime.now().isoformat(),
                filename=dataset_metadata_dict["filename"][0],
                file_size=dataset_metadata_dict["dataset_size"][0],
                full_path=dataset_metadata_dict["filename"][0],
                record_count=dataset_metadata_dict["record_count"][0],
            )
            result[metadata.name] = metadata
        return result

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

    def to_parquet(self, file_path: str) -> str:
        """
        Save the dataset with full_path == file_path to a parquet file.
        Returns the number of rows and the path to the saved parquet file, or (0, "") if not found.
        """
        for dataset in self.data:
            if hasattr(dataset, "full_path") and dataset.full_path == file_path:
                # Convert the DummyDataset's data (assumed to be a DataFrame or dict-like) to a pandas DataFrame
                if hasattr(dataset, "data"):
                    df = pd.DataFrame(dataset.data)
                else:
                    # fallback: try to convert the whole object to dict
                    df = pd.DataFrame([dataset.__dict__])
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
                df.to_parquet(temp_file.name)
                return len(df.index), temp_file.name
        return 0, ""

    @staticmethod
    def get_data(dataset_paths: Sequence[str], encoding: str = DEFAULT_ENCODING):
        json = JSONReader(encoding=encoding or DEFAULT_ENCODING).from_file(
            dataset_paths[0]
        )
        return [DummyDataset(data) for data in json.get("datasets", [])]

    @staticmethod
    def is_valid_data(dataset_paths: Sequence[str], encoding: str = DEFAULT_ENCODING):
        if (
            dataset_paths
            and len(dataset_paths) == 1
            and dataset_paths[0].lower().endswith(".json")
        ):
            json = JSONReader(encoding=encoding or DEFAULT_ENCODING).from_file(
                dataset_paths[0]
            )
            return "datasets" in json
        return False
