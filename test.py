import os

from cdisc_rules_engine.services.datasetjson_metadata_reader import DatasetJSONMetadataReader


reader = DatasetJSONMetadataReader(f"{os.path.dirname(__file__)}\\tests\\resources\\test_dataset.json", file_name="test_dataset.json")
metadata: dict = reader.read()

print(metadata["dataset_name"])
