from typing import Tuple, List

from cdisc_rules_engine.config import config
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.serializers import DatasetMetadataSerializer
from cdisc_rules_engine.services.cache import CacheServiceFactory
from cdisc_rules_engine.services.data_services import DataServiceFactory


def list_dataset_metadata_handler(dataset_paths: Tuple[str]) -> List[dict]:
    from core import VALIDATION_SUPPORTED_FORMATS

    invalid_files = []

    for path in dataset_paths:
        file_ext = path.split(".")[-1].upper()
        if file_ext not in VALIDATION_SUPPORTED_FORMATS:
            invalid_files.append((path, file_ext))

    if invalid_files:
        error_msg = "Unsupported file format(s) detected:\n"
        for file, ext in invalid_files:
            error_msg += f"  - {file} (format: {ext})\n"
        error_msg += "\nSupported formats: SAS V5 XPT or Dataset-JSON (JSON or NDJSON)"
        raise ValueError(error_msg)

    cache_service = CacheServiceFactory(config).get_service()
    data_service = DataServiceFactory(config, cache_service).get_service()
    metadata: List[SDTMDatasetMetadata] = [
        data_service.get_raw_dataset_metadata(dataset_name=path)
        for path in dataset_paths
    ]
    return DatasetMetadataSerializer(metadata).data
