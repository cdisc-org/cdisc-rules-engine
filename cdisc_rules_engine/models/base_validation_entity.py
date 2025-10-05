from abc import ABC
from dataclasses import dataclass


@dataclass
class BaseValidationEntity(ABC):
    # Remove the default to fix field ordering issues in subclasses
    pass
