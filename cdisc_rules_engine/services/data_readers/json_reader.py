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
    def from_file(self, file_path):
        # Load Dataset-JSON Schema
        with open(
            os.path.join("resources", "schema", "dataset.schema.json")
        ) as schemajson:
            schema = schemajson.read()
        schema = json.loads(schema)

        with open(file_path, "r") as file:
            datasetjson = json.load(file)

        try:
            jsonschema.validate(datasetjson, schema)
            if "clinicalData" in datasetjson:
                data_key = "clinicalData"
            elif "referenceData" in datasetjson:
                data_key = "referenceData"

            items_data = next(
                (
                    d
                    for d in datasetjson[data_key]["itemGroupData"].values()
                    if "items" in d
                ),
                {},
            )
            df = pd.DataFrame(
                [item[1:] for item in items_data.get("itemData", [])],
                columns=[item["name"] for item in items_data.get("items", [])[1:]],
            )

            df = df.applymap(lambda x: round(x, 15) if isinstance(x, float) else x)

            if self.dataset_implementation == PandasDataset:
                return PandasDataset(df)
            else:
                return DaskDataset(dd.from_pandas(df), npartitions=4)
        except jsonschema.exceptions.ValidationError:
            return PandasDataset(pd.DataFrame())

    def to_parquet(self, file_path: str) -> str:
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        dataset = self.from_file(file_path)
        dataset.data.to_parquet(temp_file.name)

    def read(self, data):
        pass
