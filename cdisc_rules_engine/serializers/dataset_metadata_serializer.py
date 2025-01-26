from typing import Iterable, List

from .base_serializer import BaseSerializer
from cdisc_rules_engine.exceptions.custom_exceptions import InvalidDatasetFormat
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata


class DatasetMetadataSerializer(BaseSerializer):
    def __init__(self, metadata: Iterable[SDTMDatasetMetadata]):
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
                    "domain": metadata_obj.domain,
                    "filename": metadata_obj.filename,
                    "full_path": metadata_obj.full_path,
                    "file_size": metadata_obj.file_size,
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
                and isinstance(metadata_obj.domain, (str, type(None)))
                and isinstance(metadata_obj.label, str)
                and isinstance(metadata_obj.modification_date, str)
                and isinstance(metadata_obj.filename, str)
                and isinstance(metadata_obj.full_path, str)
                and (
                    isinstance(metadata_obj.file_size, int)
                    or isinstance(metadata_obj.file_size, float)
                )
                and isinstance(metadata_obj.record_count, int)
            ):
                return False
        return True
