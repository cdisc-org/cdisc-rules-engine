from typing import Type

from cdisc_rules_engine.models.dictionaries import (
    DictionaryTypes,
    TermsFactoryInterface,
)
from cdisc_rules_engine.models.dictionaries.meddra import MedDRATermsFactory
from cdisc_rules_engine.models.dictionaries.whodrug import WhoDrugTermsFactory
from cdisc_rules_engine.services.data_services import BaseDataService
from cdisc_rules_engine.services.factory_interface import FactoryInterface


class AbstractTermsFactory(FactoryInterface):
    _service_map: dict = {
        DictionaryTypes.MEDDRA.value: MedDRATermsFactory,
        DictionaryTypes.WHODRUG.value: WhoDrugTermsFactory,
    }

    def __init__(self, data_service: BaseDataService):
        self.data_service = data_service

    @classmethod
    def register_service(cls, name: str, service: Type[TermsFactoryInterface]) -> None:
        if not name:
            raise ValueError("Service name must not be empty!")
        if not issubclass(service, TermsFactoryInterface):
            raise TypeError("Implementation of TermsFactoryInterface required!")
        cls._service_map[name] = service

    def get_service(self, name: str, **kwargs) -> TermsFactoryInterface:
        if name not in self._service_map:
            raise ValueError(
                f"Service name must be in  {list(self._service_map.keys())}, "
                f"given service name is {name}"
            )
        factory = self._service_map.get(name)
        return factory(data_service=self.data_service)
