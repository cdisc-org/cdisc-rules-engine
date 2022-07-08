from engine.services.blob_data_service import BlobDataService
from engine.services.local_data_service import LocalDataService
from engine.dummy_services.dummy_data_service import DummyDataService


class DataServiceFactory:
    def __init__(self, config, cache_service):
        self.config = config
        self.cache_service = cache_service

    def get_data_service(self):
        data_service_type = self.config.getValue("DATA_SERVICE_TYPE")
        if data_service_type == "azure_blob":
            return BlobDataService.get_instance(
                blob_storage_connection_string=self.config.getValue(
                    "BLOB_STORAGE_CONNECTION_STRING"
                ),
                container=self.config.getValue("CONTAINER"),
                cache_service=self.cache_service,
            )
        else:
            return LocalDataService.get_instance(
                cache_service=self.cache_service
            )

    def get_dummy_data_service(self, data):
        return DummyDataService(data)
