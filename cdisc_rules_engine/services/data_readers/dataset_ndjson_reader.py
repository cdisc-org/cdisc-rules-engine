import pandas as pd
import dask.dataframe as dd
import os
import json
import jsonschema

from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
)

from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
import tempfile

from cdisc_rules_engine.services.data_readers.json_reader import JSONReader


class DatasetNDJSONReader(DataReaderInterface):
    def __init__(self, dataset_implementation, encoding: str = None):
        self.dataset_implementation = dataset_implementation
        self.encoding = encoding

    @property
    def _encoding(self):
        return self.encoding or "utf-8"

    def get_schema(self) -> dict:
        schema = JSONReader().from_file(
            os.path.join("resources", "schema", "dataset-ndjson-schema.json")
        )
        return schema

    def read_json_file(self, file_path: str) -> dict:
        try:
            with open(file_path, "r", encoding=self._encoding) as file:
                lines = file.readlines()
            return json.loads(lines[0]), [json.loads(line) for line in lines[1:]]
        except (UnicodeDecodeError, UnicodeError) as e:
            raise ValueError(
                f"Could not decode NDJSON file {file_path} with {self._encoding} encoding: {e}. "
                f"Please specify the correct encoding using the -e flag."
            )

    def _raw_dataset_from_file(self, file_path) -> pd.DataFrame:
        # Load Dataset-JSON Schema
        schema = self.get_schema()
        metadatandjson, datandjson = self.read_json_file(file_path)

        jsonschema.validate(metadatandjson, schema)

        df = pd.DataFrame(
            [item for item in datandjson],
            columns=[item["name"] for item in metadatandjson.get("columns", [])],
        )
        return df.applymap(lambda x: round(x, 15) if isinstance(x, float) else x)

    def from_file(self, file_path):
        try:
            df = self._raw_dataset_from_file(file_path)
            if self.dataset_implementation == PandasDataset:
                return PandasDataset(df)
            else:
                return DaskDataset(
                    dd.from_pandas(df, npartitions=4), length=len(df.index)
                )
        except jsonschema.exceptions.ValidationError:
            return PandasDataset(pd.DataFrame())

    def to_parquet(self, file_path: str) -> str:
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        df = self._raw_dataset_from_file(file_path)
        df.to_parquet(temp_file.name)
        return len(df.index), temp_file.name

    def read(self, data):
        pass
