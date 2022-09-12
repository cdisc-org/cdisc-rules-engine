import pandas as pd

from cdisc_rules_engine.dummy_models.dummy_variable import DummyVariable
from cdisc_rules_engine.exceptions.custom_exceptions import InvalidDatasetFormat


class DummyDataset:
    def __init__(self, dataset_data):
        self.validate(dataset_data)
        self.name = dataset_data.get("name")
        self.label = dataset_data.get("label")
        self.filesize = dataset_data.get("filesize")
        self.filename = dataset_data.get("filename")
        self.domain = dataset_data.get("domain")
        self.variables = [
            DummyVariable(variable_data)
            for variable_data in dataset_data.get("variables", [])
        ]
        self.data = pd.DataFrame.from_dict(dataset_data.get("records", {}))

    def get_metadata(self):
        return {
            "dataset_size": [self.filesize or 1000],
            "dataset_name": [self.domain or "test"],
            "dataset_label": [self.label or "test"],
        }

    def validate(self, dataset_data):
        required_values = ["domain"]
        for value in required_values:
            if value not in dataset_data or dataset_data.get(value) is None:
                raise InvalidDatasetFormat(f"Dataset missing key: {value}")
