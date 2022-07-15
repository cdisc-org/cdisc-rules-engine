from engine.services.local_data_service import LocalDataService
from engine.dummy_services.dummy_data_service import DummyDataService


class DataServiceFactory:
    def __init__(self, config, cache_service):
        self.config = config
        self.cache_service = cache_service

    def get_data_service(self):
        # TODO extend the method as more services are available
        return LocalDataService.get_instance(cache_service=self.cache_service)

    def get_dummy_data_service(self, data):
        return DummyDataService(data)
