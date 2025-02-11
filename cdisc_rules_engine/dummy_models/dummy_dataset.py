import pandas as pd
from dataclasses import asdict
from datetime import datetime

from cdisc_rules_engine.dummy_models.dummy_variable import DummyVariable
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata


class DummyDataset(SDTMDatasetMetadata):
    def __init__(self, dataset_data: dict):
        self.name = (
            dataset_data.get("name")
            or dataset_data.get("filename").split(".")[0].upper()
        )
        self.label = dataset_data.get("label")
        self.file_size = dataset_data.get("file_size") or 0
        self.filename = dataset_data.get("filename")
        if hasattr(dataset_data, "first_record"):
            self.first_record = dataset_data.first_record
        else:
            self.first_record = {
                name: next(iter(val), None)
                for name, val in dataset_data.get("records", {}).items()
            }
        self.modification_date = datetime.now().isoformat()
        self.variables = [
            DummyVariable(variable_data)
            for variable_data in dataset_data.get("variables", [])
        ]
        self.data = pd.DataFrame.from_dict(dataset_data.get("records", {}))
        self.record_count = len(self.data.index)

    def get_metadata(self):
        return {
            "dataset_size": [self.file_size or 1000],
            "dataset_name": [self.name or "test"],
            "dataset_label": [self.label or "test"],
            "filename": [self.filename],
            "record_count": [self.record_count],
        }

    def __repr__(self):
        return asdict(self).__repr__()
