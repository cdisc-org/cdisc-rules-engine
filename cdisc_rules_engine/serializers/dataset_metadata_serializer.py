from typing import List

from .base_serializer import BaseSerializer
from cdisc_rules_engine.exceptions.custom_exceptions import InvalidDatasetFormat
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata


class DatasetMetadataSerializer(BaseSerializer):
    def __init__(self, metadata: List[DatasetMetadata]):
        self.__metadata = metadata
        if not self.is_valid:
            raise InvalidDatasetFormat(
                f"Given dataset metadata is invalid: {self.__metadata}"
            )

    @property
    def data(self) -> List[dict]:
        data = []
        for metadata_obj in self.__metadata:
            data.append(
                {
                    "domain": metadata_obj.domain_name,
                    "filename": metadata_obj.filename,
                    "full_path": metadata_obj.full_path,
                    "size": metadata_obj.size,
                    "label": metadata_obj.label,
                    "modification_date": metadata_obj.modification_date,
                }
            )
        return data

    @property
    def is_valid(self) -> bool:
        for metadata_obj in self.__metadata:
            if not (
                isinstance(metadata_obj.name, str)
                and isinstance(metadata_obj.domain_name, str)
                and isinstance(metadata_obj.label, str)
                and isinstance(metadata_obj.modification_date, str)
                and isinstance(metadata_obj.filename, str)
                and isinstance(metadata_obj.full_path, str)
                and (
                    isinstance(metadata_obj.size, int)
                    or isinstance(metadata_obj.size, float)
                )
                and isinstance(metadata_obj.records, str)
            ):
                return False
        return True
