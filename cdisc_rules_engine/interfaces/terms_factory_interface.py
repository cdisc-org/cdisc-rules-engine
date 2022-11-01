from abc import ABC, abstractmethod
from typing import Dict

from cdisc_rules_engine.interfaces import DataServiceInterface
from cdisc_rules_engine.interfaces import DictionaryTermInterface


class TermsFactoryInterface(ABC):
    """
    An interface for all factories that install dictionaries terms.
    """

    @abstractmethod
    def __init__(self, data_service: DataServiceInterface):
        """
        Initializes a factory object.
        """

    @abstractmethod
    def install_terms(
        self,
        directory_path: str,
    ) -> Dict[
        str, Dict[str, DictionaryTermInterface]
    ]:  # maps term type to a dictionary of term identifiers to term
        """
        Accepts file contents and saves it to the DB.
        """
