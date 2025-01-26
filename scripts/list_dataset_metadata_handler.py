from typing import Tuple, List

from cdisc_rules_engine.config import config
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.serializers import DatasetMetadataSerializer
from cdisc_rules_engine.services.cache import CacheServiceFactory
from cdisc_rules_engine.services.data_services import DataServiceFactory


def list_dataset_metadata_handler(dataset_paths: Tuple[str]) -> List[dict]:
    """
    Lists metadata of given datasets like:
    [
       {
          "domain":"AE",
          "filename":"ae.xpt",
          "full_path":"/Users/Aleksei_Furmenkov/PycharmProjects/cdisc-rules-engine/resources/data/ae.xpt",
          "file_size":"38000",
          "label":"Adverse Events",
          "modification_date":"2020-08-21T09:14:26"
       },
       {
          "domain":"EX",
          "filename":"ex.xpt",
          "full_path":"/Users/Aleksei_Furmenkov/PycharmProjects/cdisc-rules-engine/resources/data/ex.xpt",
          "file_size":"78050",
          "label":"Exposure",
          "modification_date":"2021-09-17T09:23:22"
       },
       ...
    ]
    """
    cache_service = CacheServiceFactory(config).get_service()
    data_service = DataServiceFactory(config, cache_service).get_service()
    metadata: List[SDTMDatasetMetadata] = [
        data_service.get_raw_dataset_metadata(dataset_name=path)
        for path in dataset_paths
    ]
    return DatasetMetadataSerializer(metadata).data
