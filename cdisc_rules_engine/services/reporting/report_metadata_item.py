from dataclasses import dataclass


@dataclass
class ReportMetadataItem:
    name: str
    row: int
    value: str | None = None
