from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder


class JsonSchemaCheckDatasetBuilder(BaseDatasetBuilder):
    def get_dataset(self):
        dataset_dict = {"json": [self.data_service.json]}
        return self.dataset_implementation.from_dict(dataset_dict)
