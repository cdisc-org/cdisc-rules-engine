from .base_serializer import BaseSerializer
from cdisc_rules_engine.models.rule import Rule
from cdisc_rules_engine.enums.rule_types import RuleTypes
from cdisc_rules_engine.enums.sensitivity import Sensitivity


class RuleSerializer(BaseSerializer):
    """
    Serializer for Rule model.
    """

    def __init__(self, rule: Rule):
        self.__rule = rule

    @property
    def data(self) -> dict:
        data: dict = {
            "category": self.__rule.category,
            "author": self.__rule.author,
            "core_id": self.__rule.core_id,
            "reference": self.__rule.reference,
            "sensitivity": self.__rule.sensitivity,
            "severity": self.__rule.severity,
            "description": self.__rule.description,
            "authority": self.__rule.authority,
            "standards": self.__rule.standards,
            "rule_type": self.__rule.rule_type,
            "conditions": self.__rule.conditions,
            "actions": self.__rule.actions,
        }

        if self.__rule.classes:
            data["classes"] = self.__rule.classes
        if self.__rule.domains:
            data["domains"] = self.__rule.domains
        if self.__rule.datasets:
            data["datasets"] = self.__rule.datasets
        if self.__rule.output_variables:
            data["output_variables"] = self.__rule.output_variables
        if self.__rule.operations:
            data["operations"] = self.__rule.operations
        return data

    @property
    def is_valid(self) -> bool:
        return (
            isinstance(self.__rule.core_id, str)
            and isinstance(self.__rule.category, str)
            and isinstance(self.__rule.author, str)
            and isinstance(self.__rule.severity, str)
            and isinstance(self.__rule.sensitivity, str)
            and Sensitivity.contains(self.__rule.sensitivity)
            and isinstance(self.__rule.description, str)
            and isinstance(self.__rule.rule_type, str)
            and RuleTypes.contains(self.__rule.rule_type)
            and (self.__rule.classes or self.__rule.domains)
        )
