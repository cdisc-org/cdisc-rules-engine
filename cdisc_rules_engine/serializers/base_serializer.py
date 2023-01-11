from abc import ABC, abstractmethod
from typing import Union, List


class BaseSerializer(ABC):
    """
    Interface for all concrete serializers.
    """

    @property
    @abstractmethod
    def data(self) -> Union[dict, List[dict]]:
        """
        Returns wrapped object(s) as a dict or list of dicts.
        """

    @property
    @abstractmethod
    def is_valid(self) -> bool:
        """
        Returns if the wrapped object is valid or not.
        """
