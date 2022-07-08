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
    async def install_terms(
        self,
        dictionary_id: str,
        directory_path: str,
        file_name: str,
        file_contents: bytes,
    ):
        """
        Accepts file contents and saves it to the DB.
        """
