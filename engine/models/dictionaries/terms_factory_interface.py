from abc import ABC, abstractmethod


class TermsFactoryInterface(ABC):
    """
    An interface for all factories that install terms.
    """

    @abstractmethod
    def __init__(self, data_service):
        """
        Initializes a factory object.
        """

    @abstractmethod
    def install_terms(
        self,
        directory_path: str,
    ) -> dict:
        """
        Accepts file contents and saves it to the DB.
        """
