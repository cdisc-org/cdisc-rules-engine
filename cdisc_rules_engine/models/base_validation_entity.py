from abc import ABC
from dataclasses import dataclass

from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.interfaces import RepresentationInterface


@dataclass
class BaseValidationEntity(RepresentationInterface, ABC):
    status: ExecutionStatus
