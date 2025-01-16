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
        self.size = dataset_data.get("filesize") or 0
        self.filename = dataset_data.get("filename")
        self.domain = next(
            iter(dataset_data.get("records", {}).get("DOMAIN", [])), None
        )
        self.rdomain = (
            next(iter(dataset_data.get("records", {}).get("RDOMAIN", [])), None)
            if self.is_supp()
            else None
        )
        self.modification_date = datetime.now().isoformat()
        self.variables = [
            DummyVariable(variable_data)
            for variable_data in dataset_data.get("variables", [])
        ]
        self.data = pd.DataFrame.from_dict(dataset_data.get("records", {}))
        self.record_count = len(self.data.index)

    def get_metadata(self):
        return {
            "dataset_size": [self.size or 1000],
            "dataset_name": [self.name or "test"],
            "dataset_label": [self.label or "test"],
            "filename": [self.filename],
            "record_count": [self.record_count],
        }

    def __repr__(self):
        return asdict(self).__repr__()
