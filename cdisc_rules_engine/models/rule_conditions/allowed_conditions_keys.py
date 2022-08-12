from cdisc_rules_engine.enums.base_enum import BaseEnum


class AllowedConditionsKeys(BaseEnum):
    """
    Stores keys allowed in the rule conditions.
    """

    ALL = "all"
    ANY = "any"
    NOT = "not"
