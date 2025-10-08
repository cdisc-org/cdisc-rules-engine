from cdisc_rules_engine.dataset_builders.values_dataset_builder import (
    ValuesDatasetBuilder,
)


class JsonSchemaCheckDatasetBuilder(ValuesDatasetBuilder):
    def get_dataset(self):
        dataset_json = {
            "json": self.data_service.json,
        }
        return dataset_json
