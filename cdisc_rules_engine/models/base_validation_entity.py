from abc import ABC
from dataclasses import dataclass

from cdisc_rules_engine.interfaces import RepresentationInterface


@dataclass
class BaseValidationEntity(RepresentationInterface, ABC):
    pass
