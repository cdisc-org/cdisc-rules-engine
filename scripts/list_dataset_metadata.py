from typing import Tuple, List

from cdisc_rules_engine.config import config
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata
from cdisc_rules_engine.serializers import DatasetMetadataSerializer
from cdisc_rules_engine.services.cache import CacheServiceFactory
from cdisc_rules_engine.services.data_services import DataServiceFactory


def list_dataset_metadata(dataset_paths: Tuple[str]) -> dict:
    """
    Lists metadata of given datasets like:
    TODO to be defined
    """
    # TODO add full path
    cache_service = CacheServiceFactory(config).get_service()
    data_service = DataServiceFactory(config, cache_service).get_service()
    metadata: List[DatasetMetadata] = [
        data_service.get_raw_dataset_metadata(dataset_name=path)
        for path in dataset_paths
    ]
    return DatasetMetadataSerializer(metadata).data
