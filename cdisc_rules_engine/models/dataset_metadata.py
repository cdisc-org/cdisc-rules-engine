from dataclasses import dataclass
from typing import Union
from os.path import basename


@dataclass
class DatasetMetadata:
    """
    This class is a container for dataset metadata
    """

    name: str = ""
    label: str = ""
    filename: str = ""
    file_size: Union[int, float] = 0
    record_count: int = 0
    modification_date: str = ""
    full_path: Union[str, None] = None
    first_record: Union[dict, None] = None
    original_path: Union[str, None] = None

    @property
    def data_service_identifier(self) -> str:
        return basename(self.full_path) if self.full_path else self.filename
