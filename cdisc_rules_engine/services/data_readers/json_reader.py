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


class DatasetJSONReader(DataReaderInterface):
    def get_schema(self) -> dict:
        with open(
            os.path.join("resources", "schema", "dataset.schema.json")
        ) as schemajson:
            schema = schemajson.read()
        return json.loads(schema)

    def get_data_key(self, dataset_json: dict) -> str:
        if "clinicalData" in dataset_json:
            return "clinicalData"
        return "referenceData"

    def read_json_file(self, file_path: str) -> dict:
        with open(file_path, "r") as file:
            datasetjson = json.load(file)
        return datasetjson

    def parse_items_data(self, dataset_json: dict, data_key: str) -> pd.DataFrame:
        items_data = next(
            (
                d
                for d in dataset_json[data_key]["itemGroupData"].values()
                if "items" in d
            ),
            {},
        )
        return pd.DataFrame(
            [item[1:] for item in items_data.get("itemData", [])],
            columns=[item["name"] for item in items_data.get("items", [])[1:]],
        )

    def _raw_dataset_from_file(self, file_path) -> pd.DataFrame:
        # Load Dataset-JSON Schema
        schema = self.get_schema()
        datasetjson = self.read_json_file(file_path)

        jsonschema.validate(datasetjson, schema)
        data_key = self.get_data_key(datasetjson)
        df = self.parse_items_data(datasetjson, data_key)
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

    def to_parquet(self, file_path: str) -> (int, str):
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        df = self._raw_dataset_from_file(file_path)
        df.to_parquet(temp_file.name)
        return len(df.index), temp_file.name

    def read(self, data):
        pass
