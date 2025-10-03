from dataclasses import dataclass, field
from typing import List


@dataclass
class VariableMetadataContainer:
    formats: List = field(init=False)
    names: List = field(init=False)
    order: List = field(init=False)
    labels: List = field(init=False)
    sizes: List = field(init=False)
    data_types: List = field(init=False)
    
    def __init__(self, contents_metadata: dict):
        variable_names = contents_metadata["variable_names"]
        self.formats = contents_metadata["variable_formats"]
        self.names = variable_names
        self.order = [(variable_names.index(name) + 1) for name in variable_names]
        self.labels = contents_metadata["variable_name_to_label_map"].values()
        self.sizes = contents_metadata["variable_name_to_size_map"].values()
        self.data_types = contents_metadata["variable_name_to_data_type_map"].values()

    def as_dict(self) -> dict:
        return {
            "variable_name": self.names,
            "variable_order_number": self.order,
            "variable_label": self.labels,
            "variable_size": self.sizes,
            "variable_data_type": self.data_types,
            "variable_format": self.formats,
        }
