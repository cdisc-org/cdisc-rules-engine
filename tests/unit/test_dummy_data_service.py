import numpy as np

from engine.dummy_services.dummy_data_service import DummyDataService
from engine.dummy_models.dummy_dataset import DummyDataset
import pandas as pd
import pytest


def test_get_dataset():
    dataset_data = [
        {
            "domain": "AE",
            "filename": "ae.xpt",
            "name": "AE",
            "records": {
                "AESEQ": [1, 2, 3, 4, ],
                "AENUM": [1.0, 2.0, 3.0, 4.0, ],
                "AEDY": [1, np.nan, np.nan, np.nan, ],
                "AEORNRLO": ["", "", "", "TEST", ],
                "AESTNRHI": [None, None, None, None, ],
            },
        }
    ]
    datasets = [DummyDataset(dataset) for dataset in dataset_data]
    data_service = DummyDataService(datasets)
    dataset = data_service.get_dataset("ae.xpt")
    assert isinstance(dataset, pd.DataFrame)
    assert dataset["AESEQ"].to_list() == [1, 2, 3, 4, ]
    assert dataset["AEDY"].to_list() == [1, None, None, None, ]
    assert dataset["AENUM"].to_list() == [1.0, 2.0, 3.0, 4.0, ]
    assert dataset["AENUM"].dtype == "float64"
    assert dataset["AEORNRLO"].to_list() == ["", "", "", "TEST", ]
    assert dataset["AESTNRHI"].to_list() == [None, None, None, None, ]


def test_get_dataset_metadata():
    dataset_data = [
        {
            "name": "AE",
            "filesize": 2000,
            "filename": "ae.xpt",
            "label": "ADVERSE EVENTS",
            "domain": "AE",
            "records": {"AESEQ": [1, 2, 3, 4]},
        }
    ]
    datasets = [DummyDataset(dataset) for dataset in dataset_data]
    data_service = DummyDataService(datasets)
    metadata = data_service.get_dataset_metadata("ae.xpt")
    assert isinstance(metadata, pd.DataFrame)
    assert metadata["dataset_label"].iloc[0] == "ADVERSE EVENTS"
    assert metadata["dataset_name"].iloc[0] == "AE"
    assert metadata["dataset_size"].iloc[0] == 2000


def test_get_variables_metadata():
    dataset_data = [
        {
            "name": "AE",
            "filename": "ae.xpt",
            "filesize": 2000,
            "label": "ADVERSE EVENTS",
            "domain": "AE",
            "variables": [
                {
                    "name": "AESEQ",
                    "label": "AE Sequence",
                    "type": "integer",
                    "length": 5,
                }
            ],
            "records": {"AESEQ": [1, 2, 3, 4]},
        }
    ]
    datasets = [DummyDataset(dataset) for dataset in dataset_data]
    data_service = DummyDataService(datasets)
    metadata = data_service.get_variables_metadata("/ae.xpt")
    assert isinstance(metadata, pd.DataFrame)
    assert metadata["variable_name"].iloc[0] == "AESEQ"
    assert metadata["variable_label"].iloc[0] == "AE Sequence"
    assert metadata["variable_data_type"].iloc[0] == "integer"
    assert metadata["variable_size"].iloc[0] == 5
    assert metadata["variable_order"].iloc[0] == 1
