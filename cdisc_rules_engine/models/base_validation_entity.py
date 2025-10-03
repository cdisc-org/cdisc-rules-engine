from abc import ABC
from dataclasses import dataclass
from cdisc_rules_engine.enums.execution_status import ExecutionStatus


@dataclass
class BaseValidationEntity(ABC):
    status: ExecutionStatus | None = None
