from cdisc_rules_engine.exceptions.custom_exceptions import RuleFormatError
from cdisc_rules_engine.interfaces import ConditionInterface

from .allowed_conditions_keys import AllowedConditionsKeys
from .condition_composite import ConditionComposite
from .not_condition_composite import NotConditionComposite
from .single_condition import SingleCondition


class ConditionCompositeFactory:
    """
    Builds the composite from the given rule conditions.
    This class is an entrypoint for the client code.
    """

    @classmethod
    def get_condition_composite(cls, conditions: dict) -> ConditionInterface:
        composite = ConditionComposite()
        for key, condition_list in conditions.items():
            # validate the rule structure
            if not AllowedConditionsKeys.contains(key):
                raise RuleFormatError(
                    f'Key "{key}" is not allowed in the rule conditions'
                )

            if key == AllowedConditionsKeys.NOT.value:
                # "not" composite wraps a regular composite
                return NotConditionComposite(
                    key=key,
                    condition_composite=cls.get_condition_composite(conditions["not"]),
                )
            else:
                # create a regular composite
                conditions_to_add = []
                for condition in condition_list:
                    if cls._is_nested_condition(condition):
                        conditions_to_add.append(cls.get_condition_composite(condition))
                    else:
                        conditions_to_add.append(SingleCondition(condition))
                composite.add_conditions(key, conditions_to_add)
        return composite

    @staticmethod
    def _is_nested_condition(condition: dict) -> bool:
        return any(key in condition for key in AllowedConditionsKeys.values())
