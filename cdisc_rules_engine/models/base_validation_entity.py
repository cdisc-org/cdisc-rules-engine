from abc import ABC
from dataclasses import dataclass
from typing import Optional


@dataclass
class BaseValidationEntity(ABC):
    status: Optional = None
    
    def as_dict(self) -> dict:
        """
        Default implementation that can be overridden by subclasses.
        Returns a basic dict representation.
        """
        return {"status": self.status}
