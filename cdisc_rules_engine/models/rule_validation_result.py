from typing import List
from dataclasses import dataclass, field

from cdisc_rules_engine.interfaces import RepresentationInterface
from cdisc_rules_engine.utilities.utils import get_execution_status
from cdisc_rules_engine.models.rule import Rule


@dataclass
class RuleValidationResult(RepresentationInterface):
    id: str | None = None
    cdisc_rule_id: str | None = None
    fda_rule_id: str | None = None
    executability: str | None = None
    message: str | None = None
    execution_status: str | None = None
    results: List[dict | str] = field(default_factory=list)

    def __init__(self, rule: Rule, results: List[dict | str]):
        self.id = rule.get("core_id")
        self.cdisc_rule_id = self._get_rule_ids(rule, "CDISC")
        self.fda_rule_id = self._get_rule_ids(rule, "FDA")
        self.executability = rule.get("executability")
        actions = rule.get("actions")
        if actions and len(actions) == 1:
            self.message = actions[0].get("params", {}).get("message")
        self.execution_status = get_execution_status(results)
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

    def to_representation(self) -> dict:
        return {
            "id": self.id,
            "executability": self.executability,
            "execution_status": self.execution_status,
            "message": self.message,
            "results": self.results,
        }
