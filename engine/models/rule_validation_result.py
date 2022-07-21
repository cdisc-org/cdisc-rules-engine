from engine.models.representation_interface import RepresentationInterface
from engine.utilities.utils import get_execution_status
from typing import List, Union


class RuleValidationResult(RepresentationInterface):
    def __init__(self, rule, results: List[Union[dict, str]]):
        self.id: str = rule.get("core_id")
        self.severity: str = rule.get("severity")
        self.message: str = rule.get("message")
        self.execution_status: str = get_execution_status(results)
        self.results = results

    def to_representation(self) -> dict:
        return {
            "id": self.id,
            "severity": self.severity,
            "execution_status": self.execution_status,
            "message": self.message,
            "results": self.results,
        }
