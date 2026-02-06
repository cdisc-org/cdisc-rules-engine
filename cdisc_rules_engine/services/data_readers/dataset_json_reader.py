import pandas as pd
import dask.dataframe as dd
import os
import jsonschema

from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
)

from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
import tempfile

from cdisc_rules_engine.services.data_readers.json_reader import JSONReader
from cdisc_rules_engine.constants import DEFAULT_ENCODING


class DatasetJSONReader(DataReaderInterface):

    def get_schema(self) -> dict:
        schema = JSONReader(encoding=DEFAULT_ENCODING).from_file(
            os.path.join("resources", "schema", "dataset.schema.json")
        )
        return schema

    def read_json_file(self, file_path: str) -> dict:
        return JSONReader(encoding=self.encoding).from_file(file_path)

    def _raw_dataset_from_file(self, file_path) -> pd.DataFrame:
        # Load Dataset-JSON Schema
        schema = self.get_schema()
        datasetjson = self.read_json_file(file_path)

        jsonschema.validate(datasetjson, schema)

        df = pd.DataFrame(
            [item for item in datasetjson.get("rows", [])],
            columns=[item["name"] for item in datasetjson.get("columns", [])],
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
