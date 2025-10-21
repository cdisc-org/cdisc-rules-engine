from dataclasses import dataclass
from typing import Dict, List, Union

from cdisc_rules_engine.models.dataset_metadata2 import (
    DatasetMetadata2,
    VariableMetadata,
)


@dataclass
class TestDataset(DatasetMetadata2):
    records: Dict[str, List[Union[str, int, float]]]

    @staticmethod
    def from_records(name: str, column_data: Dict[str, List[Union[str, int, float]]]) -> "TestDataset":
        lengths = {len(v) for v in column_data.values()}
        if len(set(lengths)) != 1:
            raise ValueError("All input data columns must have the same length")

        return TestDataset(
            filename=f"{name}.xpt",
            name=name,
            label=f"Test {name} Dataset",
            variables=[
                VariableMetadata(
                    name=col,
                    order=i + 1,
                    label=f"Test {col} Variable",
                    length=200,
                    type="Char" if isinstance(next((val for val in values if val is not None), ""), str) else "Num",
                    format="",
                )
                for i, (col, values) in enumerate(column_data.items())
            ],
            records=column_data,
        )
