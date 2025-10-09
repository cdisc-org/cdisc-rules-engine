from cdisc_rules_engine.dataset_builders.values_dataset_builder import (
    ValuesDatasetBuilder,
)
from cdisc_rules_engine.models.dataset import PandasDataset


class JsonSchemaCheckDatasetBuilder(ValuesDatasetBuilder):
    def get_dataset(self):
        dataset_dict = {"json": [self.data_service.json]}
        return PandasDataset.from_dict(dataset_dict)
