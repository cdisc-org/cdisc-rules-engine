from typing import Iterable, List, Type

from cdisc_rules_engine.dummy_models.dummy_dataset import DummyDataset
from cdisc_rules_engine.interfaces import (
    CacheServiceInterface,
    ConfigInterface,
    DataServiceInterface,
    FactoryInterface,
)
from cdisc_rules_engine.models.dataset import DaskDataset, PandasDataset


from . import DummyDataService, LocalDataService, USDMDataService
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.services import logger


class DataServiceFactory(FactoryInterface):
    _registered_services_map = {
        "local": LocalDataService,
        "dummy": DummyDataService,
        "usdm": USDMDataService,
    }

    def __init__(
        self,
        config: ConfigInterface,
        cache_service: CacheServiceInterface,
        standard: str = None,
        standard_version: str = None,
        standard_substandard: str = None,
        library_metadata: LibraryMetadataContainer = None,
        max_dataset_size: int = 0,
    ):
        if config.getValue("DATA_SERVICE_TYPE"):
            self.data_service_name = config.getValue("DATA_SERVICE_TYPE")
        elif standard == "usdm":
            self.data_service_name = "usdm"
        else:
            self.data_service_name = "local"
        self.config = config
        self.cache_service = cache_service
        self.standard = standard
        self.standard_version = standard_version
        self.standard_substandard = standard_substandard
        self.library_metadata = library_metadata
        self.max_dataset_size = max_dataset_size
        self.dataset_size_threshold = self.config.get_dataset_size_threshold()

    def get_data_service(
        self, dataset_paths: Iterable[str] = []
    ) -> DataServiceInterface:
        if USDMDataService.is_USDM_data(dataset_paths):
            """Get json file tree to dataset data service"""
            return self.get_service(
                "usdm",
                standard=self.standard,
                standard_version=self.standard_version,
                standard_substandard=self.standard_substandard,
                library_metadata=self.library_metadata,
                dataset_path=dataset_paths[0],
                dataset_implementation=self.get_dataset_implementation(),
            )
        else:
            """Get local Directory data service"""
            return self.get_service(
                "local",
                standard=self.standard,
                standard_version=self.standard_version,
                standard_substandard=self.standard_substandard,
                library_metadata=self.library_metadata,
                dataset_paths=dataset_paths,
                dataset_implementation=self.get_dataset_implementation(),
            )

    def get_dummy_data_service(self, data: List[DummyDataset]) -> DataServiceInterface:
        return self.get_service(
            "dummy",
            data=data,
            standard=self.standard,
            standard_version=self.standard_version,
            standard_substandard=self.standard_substandard,
            library_metadata=self.library_metadata,
            dataset_implementation=self.get_dataset_implementation(),
        )

    def get_dataset_implementation(self):
        """
        Gets the class that should be used to represent datasets for the rules engine. This class may be dependent on
        rule size or config values

        :returns DatasetInterface.__class__
        """
        if (
            self.max_dataset_size
            and self.max_dataset_size >= self.dataset_size_threshold
            and self.data_service_name != "usdm"
        ):
            # Use large dataset class
            logger.info("Using DASK dataset implementation")
            return DaskDataset
        logger.info("Using PANDAS dataset implementation")
        return PandasDataset

    @classmethod
    def register_service(cls, name: str, service: Type[DataServiceInterface]) -> None:
        """
        Save mapping of service name and it's implementation
        """
        if not name:
            raise ValueError("Service name must not be empty!")
        if not issubclass(service, DataServiceInterface):
            raise TypeError("Implementation of DataServiceInterface required!")
        cls._registered_services_map[name] = service

    def get_service(self, name: str = None, **kwargs) -> DataServiceInterface:
        """Get instance of service that matches searched implementation"""
        service_name = name or self.data_service_name
        if service_name in self._registered_services_map:
            return self._registered_services_map.get(service_name).get_instance(
                config=self.config, cache_service=self.cache_service, **kwargs
            )
        raise ValueError(
            f"Service name must be in  {list(self._registered_services_map.keys())}, "
            f"given service name is {service_name}"
        )
