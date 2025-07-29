from typing import TypedDict, List, Dict, Literal, Union


class TestVariableMetadata(TypedDict):
    name: str
    label: str
    type: Literal["Char", "Num"]
    length: int
    format: str


class TestDataset(TypedDict):
    filename: str
    filepath: str
    name: str
    label: str
    variables: List[TestVariableMetadata]
    records: Dict[str, List[Union[str, int, float]]]
