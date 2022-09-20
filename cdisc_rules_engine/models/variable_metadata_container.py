from cdisc_rules_engine.interfaces import RepresentationInterface


class VariableMetadataContainer(RepresentationInterface):
    def __init__(self, contents_metadata: dict):
        variable_names = contents_metadata["variable_names"]
        self.names = variable_names
        self.order = [(variable_names.index(name) + 1) for name in variable_names]
        self.labels = contents_metadata["variable_name_to_label_map"].values()
        self.sizes = contents_metadata["variable_name_to_size_map"].values()
        self.data_types = contents_metadata["variable_name_to_data_type_map"].values()

    def to_representation(self) -> dict:
        return {
            "variable_name": self.names,
            "variable_order": self.order,
            "variable_label": self.labels,
            "variable_size": self.sizes,
            "variable_data_type": self.data_types,
        }
