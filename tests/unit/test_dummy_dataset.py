import pytest
import os
import json
import jsonschema

from cdisc_rules_engine.dummy_models.dummy_dataset import DummyDataset
from cdisc_rules_engine.exceptions.custom_exceptions import InvalidDatasetFormat


def test_invalid_dataset_data():
    dataset_data = [
        {
            "datasets": [{
                "domain": "AE",
                "filename": "ae.xpt",
                "name": "ae.xpt",
                "label": "Adverse Events",
                "variables": [{
                                "name": "AESEQ",
                                "label": "Sequence Number",
                                "type": "Num",
                                "length": 8
                                }
                ],
                "records": {"AESEQ": [1, 2, 3, 4]}
            }]
        },
        {
            "creationDateTime": "2023-07-31T14:44:06",
            "datasetJSONVersion": "1.0.0",
            "anyData": {
                "itemGroupData": {
                    "AE": {
                        "records": 4,
                        "name": "AE",
                        "label": "Adverse Events",
                        "items": [
                            {
                                "OID": "ITEMGROUPDATASEQ",
                                "name": "ITEMGROUPDATASEQ",
                                "label": "Record identifier",
                                "type": "integer"
                            },
                            {
                                "OID": "IT.AE.AESEQ",
                                "name": "AESEQ",
                                "label": "Sequence Number",
                                "type": "integer",
                                "length": 8
                            }
                        ],
                        "itemData": [
                            [1,1],
                            [2,2],
                            [3,3],
                            [4,4]
                            ]
                    }
                }
            }
        }
    ]

    _json_format = {
        "editor": "editorDataset.schema.json",
        "datasetjson": "dataset.schema.json",
    }
    
    with pytest.raises(InvalidDatasetFormat):
        schema_path: str = (
                f"{os.path.dirname(__file__)}/../../resources/schema"
            )
        for format in _json_format:
            try:
                with open(
                    os.path.join(schema_path, _json_format[format])
                ) as schema_file:
                    schema = schema_file.read()
                schema = json.loads(schema)
                jsonschema.validate(dataset_data[0], schema)
                if format == "editor":
                    return [DummyDataset(dataset_data[0])]
                elif format == "datasetjson":
                    return [DummyDataset(dataset_data[0])]
            except jsonschema.exceptions.ValidationError:
                pass
        raise InvalidDatasetFormat(f"Invalid dataset format for file: xxxx")
   
    with pytest.raises(InvalidDatasetFormat):
        schema_path: str = (
                f"{os.path.dirname(__file__)}/../../resources/schema"
            )
        for format in _json_format:
            try:
                with open(
                    os.path.join(schema_path, _json_format[format])
                ) as schema_file:
                    schema = schema_file.read()
                schema = json.loads(schema)
                jsonschema.validate(dataset_data[1], schema)
                if format == "editor":
                    return [DummyDataset(dataset_data[1])]
                elif format == "datasetjson":
                    return [DummyDataset(dataset_data[1])]
            except jsonschema.exceptions.ValidationError:
                pass
        raise InvalidDatasetFormat(f"Invalid dataset format for file: xxxx")


def test_valid_dataset_data():
    dataset_data = [
        {
            "domain": "AE",
            "filename": "ae.xpt",
            "label": "Adverse Events",
            "records": {"AESEQ": [1, 2, 3, 4]},
        },
        {
            "creationDateTime": "2023-07-31T14:44:06",
            "datasetJSONVersion": "1.0.0",
            "clinicalData": {
                "itemGroupData": {
                    "AE": {
                        "records": 4,
                        "name": "AE",
                        "label": "Adverse Events",
                        "items": [
                            {
                                "OID": "ITEMGROUPDATASEQ",
                                "name": "ITEMGROUPDATASEQ",
                                "label": "Record identifier",
                                "type": "integer"
                            },
                            {
                                "OID": "IT.AE.DOMAIN",
                                "name": "DOMAIN",
                                "label": "Domain Abbreviation",
                                "type": "string",
                                "length": 2
                            },
                            {
                                "OID": "IT.AE.AESEQ",
                                "name": "AESEQ",
                                "label": "Sequence Number",
                                "type": "integer",
                                "length": 8
                            }
                        ],
                        "itemData": [
                            [1,"AE",1],
                            [2,"AE",2],
                            [3,"AE",3],
                            [4,"AE",4]
                            ]
                    }
                }
            }
        }
    ]

    dataset1 = DummyDataset(dataset_data[0])
    assert dataset1.domain == "AE"
    assert dataset1.label == "Adverse Events"

    dataset2 = DummyDataset(dataset_data[1])
    assert dataset2.domain == "AE"
    assert dataset2.label == "Adverse Events"


def test_get_dataset_metadata():
    dataset_data = [
        {
            "domain": "AE",
            "filename": "ae.xpt",
            "label": "Adverse Events",
            "records": {"AESEQ": [1, 2, 3, 4]},
        },
        {
            "creationDateTime": "2023-07-31T14:44:06",
            "datasetJSONVersion": "1.0.0",
            "clinicalData": {
                "itemGroupData": {
                    "AE": {
                        "records": 4,
                        "name": "AE",
                        "label": "Adverse Events",
                        "items": [
                            {
                                "OID": "ITEMGROUPDATASEQ",
                                "name": "ITEMGROUPDATASEQ",
                                "label": "Record identifier",
                                "type": "integer"
                            },
                            {
                                "OID": "IT.AE.DOMAIN",
                                "name": "DOMAIN",
                                "label": "Domain Abbreviation",
                                "type": "string",
                                "length": 2
                            },
                            {
                                "OID": "IT.AE.AESEQ",
                                "name": "AESEQ",
                                "label": "Sequence Number",
                                "type": "integer",
                                "length": 8
                            }
                        ],
                        "itemData": [
                            [1,"AE",1],
                            [2,"AE",2],
                            [3,"AE",3],
                            [4,"AE",4]
                            ]
                    }
                }
            }
        }
    ]
    dataset1 = DummyDataset(dataset_data[0])
    metadata1 = dataset1.get_metadata()
    assert "dataset_name" in metadata1
    assert metadata1["dataset_name"] == ["AE"]
    assert metadata1["dataset_label"] == ["Adverse Events"]

    dataset2 = DummyDataset(dataset_data[1])
    metadata2 = dataset2.get_metadata()
    assert "dataset_name" in metadata2
    assert metadata2["dataset_name"] == ["AE"]
    assert metadata2["dataset_label"] == ["Adverse Events"]
