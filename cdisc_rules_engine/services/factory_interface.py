from abc import ABC, abstractmethod


class FactoryInterface(ABC):
    @classmethod
    @abstractmethod
    def register_service(cls, name: str, service) -> None:
        pass

    @classmethod
    @abstractmethod
    def get_service(cls, name: str, **kwargs):
        pass
