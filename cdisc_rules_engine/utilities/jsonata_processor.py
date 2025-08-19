from jsonata import Jsonata

from cdisc_rules_engine.models.rule_conditions.condition_composite import (
    ConditionComposite,
)
from cdisc_rules_engine.models.rule_conditions.single_condition import SingleCondition


class JSONataProcessor:

    @staticmethod
    def execute_jsonata_rule(rule, dataset, datasets, dataset_metadata, **kwargs):
        conditions: ConditionComposite = rule.get("conditions")
        condition: SingleCondition = conditions.get_conditions().get("all", [])[0]
        check = condition.get_conditions().get("operator")
        expr = Jsonata(check)
        result = expr.evaluate(dataset)
        return result
