from typing import Type

from cdisc_rules_engine.interfaces import (
    DataServiceInterface,
    FactoryInterface,
    TermsFactoryInterface,
)
from .dictionary_types import (
    DictionaryTypes,
)
from .meddra import MedDRATermsFactory
from .whodrug import WhoDrugTermsFactory


class AbstractTermsFactory(FactoryInterface):
    _registered_services_map: dict = {
        DictionaryTypes.MEDDRA.value: MedDRATermsFactory,
        DictionaryTypes.WHODRUG.value: WhoDrugTermsFactory,
    }

    def __init__(self, data_service: DataServiceInterface):
        self.data_service = data_service

    @classmethod
    def register_service(cls, name: str, service: Type[TermsFactoryInterface]) -> None:
        if not name:
            raise ValueError("Service name must not be empty!")
        if not issubclass(service, TermsFactoryInterface):
            raise TypeError("Implementation of TermsFactoryInterface required!")
        cls._registered_services_map[name] = service

    def get_service(self, name: str, **kwargs) -> TermsFactoryInterface:
        if name not in self._registered_services_map:
            raise ValueError(
                f"Service name must be in"
                f" {list(self._registered_services_map.keys())}, "
                f"given service name is {name}"
            )
        factory = self._registered_services_map.get(name)
        return factory(data_service=self.data_service)
