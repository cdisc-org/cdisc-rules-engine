from json import load
from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder


class JSONataDatasetBuilder(BaseDatasetBuilder):

    def get_dataset(self, **kwargs):
        with self.data_service.read_data(self.data_service.dataset_path) as fp:
            json = load(fp)
        return json
