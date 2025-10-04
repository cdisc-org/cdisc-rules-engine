from typing import List
from dataclasses import dataclass

from cdisc_rules_engine.utilities.utils import get_execution_status
from cdisc_rules_engine.models.rule import Rule
from cdisc_rules_engine.enums.execution_status import ExecutionStatus


@dataclass
class RuleValidationResult:
    id: str = None
    cdisc_rule_id: str = None
    fda_rule_id: str = None
    executability: str = None
    message: str = None
    execution_status: ExecutionStatus = None
    results: List[dict | str] = None

    def __init__(self, rule: Rule, results: List[dict | str]):
        self.id = rule.get("core_id")
        self.cdisc_rule_id = self._get_rule_ids(rule, "CDISC")
        self.fda_rule_id = self._get_rule_ids(rule, "FDA")
        self.executability = rule.get("executability")
        actions = rule.get("actions")
        self.message = None
        if actions and len(actions) == 1:
            self.message = actions[0].get("params", {}).get("message")
        status_value = get_execution_status(results)
        # Find the ExecutionStatus enum by its string value
        for status in ExecutionStatus:
            if status.value == status_value:
                self.execution_status = status
                break
        self.results = results

    def _get_rule_ids(self, rule: Rule, org: str) -> str:
        return ", ".join(
            sorted(
                {
                    reference.get("Rule_Identifier", {}).get("Id")
                    for authority in rule.get("authorities", [])
                    for standard in authority.get("Standards", [])
                    for reference in standard.get("References", [])
                    if authority.get("Organization") == org
                }
            )
        )

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "executability": self.executability,
            "execution_status": self.execution_status,
            "message": self.message,
            "results": self.results,
        }
