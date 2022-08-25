from abc import ABC, abstractmethod


class FactoryInterface(ABC):
    @classmethod
    @abstractmethod
    def register_service(cls, name: str, service) -> None:
        pass

    @abstractmethod
    def get_service(self, name: str, **kwargs):
        pass
