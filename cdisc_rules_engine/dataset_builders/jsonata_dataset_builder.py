from json import load
from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder


class JSONataDatasetBuilder(BaseDatasetBuilder):

    def get_dataset(self, **kwargs):
        if hasattr(self.data_service, "dataset_path"):
            dataset_path = self.data_service.dataset_path
        elif (
            hasattr(self.data_service, "dataset_paths")
            and len(self.data_service.dataset_paths) == 1
        ):
            dataset_path = self.data_service.dataset_paths[0]
        else:
            return None
        with self.data_service.read_data(dataset_path) as fp:
            json = load(fp)
        return json
