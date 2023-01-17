from typing import List, Union

from cdisc_rules_engine.interfaces import RepresentationInterface
from cdisc_rules_engine.utilities.utils import get_execution_status


class RuleValidationResult(RepresentationInterface):
    def __init__(self, rule, results: List[Union[dict, str]]):
        self.id: str = rule.get("core_id")
        self.executability: str = rule.get("executability")
        self.message: str = rule.get("message")
        self.execution_status: str = get_execution_status(results)
        self.results = results

    def to_representation(self) -> dict:
        return {
            "id": self.id,
            "executability": self.executability,
            "execution_status": self.execution_status,
            "message": self.message,
            "results": self.results,
        }
