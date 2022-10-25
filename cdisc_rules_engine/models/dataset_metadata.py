from dataclasses import dataclass

from cdisc_rules_engine.interfaces import RepresentationInterface


@dataclass
class DatasetMetadata(RepresentationInterface):
    """
    This class is a container for dataset metadata
    """

    name: str
    domain_name: str
    label: str
    modification_date: str
    filename: str
    size: str
    records: str

    def to_representation(self) -> dict:
        return {
            "name": self.name,
            "domainName": self.domain_name,
            "label": self.label,
            "modificationDate": self.modification_date,
            "fileName": self.filename,
            "size": self.size,
            "records": self.records,
        }
