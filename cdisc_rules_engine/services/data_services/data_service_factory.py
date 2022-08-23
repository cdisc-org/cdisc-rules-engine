from cdisc_rules_engine.services.data_services import DummyDataService

from .local_data_service import LocalDataService


class DataServiceFactory:
    def __init__(self, config, cache_service):
        self.config = config
        self.cache_service = cache_service

    def get_data_service(self):
        # TODO extend the method as more services are available
        return LocalDataService.get_instance(cache_service=self.cache_service)

    def get_dummy_data_service(self, data):
        return DummyDataService(data)
