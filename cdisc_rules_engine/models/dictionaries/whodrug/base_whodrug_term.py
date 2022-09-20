from abc import abstractmethod

from cdisc_rules_engine.interfaces import RepresentationInterface


class BaseWhoDrugTerm(RepresentationInterface):
    """
    This class contains some common implementation
    between all WhoDrug terms.
    """

    def __init__(self, record_params: dict):
        self.type: str = record_params["type"]
        self.code: str = record_params["code"]

    @classmethod
    @abstractmethod
    def from_txt_line(cls, line: str) -> "BaseWhoDrugTerm":
        """
        Creates an instance from the given line.
        Does not save it to the DB.
        """

    def to_representation(self):
        return {"code": self.code, "type": self.type}
