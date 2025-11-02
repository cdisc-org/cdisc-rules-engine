from json import load
from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder


def add_json_pointer_paths(node, path=""):
    """
    Recursively adds a '_path' attribute to each dict node in the JSON structure,
    using JSON Pointer syntax.
    """
    if isinstance(node, dict):
        node["_path"] = path
        for key, value in node.items():
            if key != "_path":
                add_json_pointer_paths(value, f"{path}/{key}")
    elif isinstance(node, list):
        for idx, item in enumerate(node):
            add_json_pointer_paths(item, f"{path}/{idx}")


class JSONataDatasetBuilder(BaseDatasetBuilder):

    def get_dataset(self, **kwargs):
        if not self.dataset_metadata.full_path:
            return None
        with self.data_service.read_data(self.dataset_metadata.full_path) as fp:
            json = load(fp)
        add_json_pointer_paths(json)
        return json
