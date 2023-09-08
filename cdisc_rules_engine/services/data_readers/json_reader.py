
import pandas as pd
import os
import json
import jsonschema

from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
)


class DatasetJSONReader(DataReaderInterface):
    def from_file(self, file_path):
        
        # Load Dataset-JSON Schema
        with open(os.path.join("resources", "schema", "dataset.schema.json")) as schemajson:
            schema = schemajson.read()
        schema = json.loads(schema)

        with open(file_path, 'r') as file:
            datasetjson = json.load(file)

        valid = True
        try:
            jsonschema.validate(datasetjson, schema)

        except jsonschema.exceptions.ValidationError as e:
            valid = False
            return pd.DataFrame()
        
        if valid:
            if "clinicalData" in datasetjson:
                data_key = "clinicalData"
            elif "referenceData" in datasetjson:
                data_key = "referenceData"

            items_data = next((d for d in datasetjson[data_key]["itemGroupData"].values() if "items" in d), {})
            df = pd.DataFrame([item[1:] for item in items_data.get("itemData", [])], columns=[item["name"] for item in items_data.get("items", [])[1:]])

            df = df.applymap(lambda x: round(x, 15) if isinstance(x, float) else x)

            return df

    def read(self, data):
        pass
