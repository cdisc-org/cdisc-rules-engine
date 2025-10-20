from dataclasses import asdict
from datetime import datetime

import pandas as pd

from cdisc_rules_engine.dummy_models.dummy_variable import DummyVariable
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.test_dataset import TestDataset


class DummyDataset(SDTMDatasetMetadata):
    def __init__(self, dataset_data: dict):
        # with XPT in test, we pass the dataset_data as an instance of SDTMDatasetMetadata
        if isinstance(dataset_data, SDTMDatasetMetadata):
            super().__init__(
                name=dataset_data.name,
                label=dataset_data.label,
                filename=dataset_data.filename,
                file_size=dataset_data.file_size,
                record_count=dataset_data.record_count,
                modification_date=dataset_data.modification_date,
                full_path=dataset_data.full_path,
                first_record=dataset_data.first_record,
            )
        elif isinstance(dataset_data, TestDataset):
            super().__init__(
                name=dataset_data.name,
                label=dataset_data.label,
                filename=dataset_data.name.lower() + ".xpt",
                file_size=0,
                record_count=0,
                modification_date=None,
                full_path=dataset_data.name.lower() + ".xpt",
                first_record={k: v[0] for k, v in dataset_data.records.items()},
            )
            self.variables = dataset_data.variables
            self.data = pd.DataFrame.from_dict(dataset_data.records)
            self.record_count = len(self.data.index)
        else:
            self.name = dataset_data.get("name") or dataset_data.get("filename").split(".")[0].upper()
            self.label = dataset_data.get("label")
            self.file_size = dataset_data.get("file_size") or 0
            self.filename = dataset_data.get("filename")
            self.full_path = dataset_data.get("filename")
            if hasattr(dataset_data, "first_record"):
                self.first_record = dataset_data.first_record
            else:
                self.first_record = {
                    name: next(iter(val), None) for name, val in dataset_data.get("records", {}).items()
                }
            self.modification_date = datetime.now().isoformat()
            self.variables = [DummyVariable(variable_data) for variable_data in dataset_data.get("variables", [])]
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
