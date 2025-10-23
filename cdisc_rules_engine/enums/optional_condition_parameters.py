from cdisc_rules_engine.enums.base_enum import BaseEnum


class OptionalConditionParameters(BaseEnum):
    DATE_COMPONENT = "date_component"
    PREFIX = "prefix"
    SUFFIX = "suffix"
    CONTEXT = "context"
    VALUE_IS_LITERAL = "value_is_literal"
    WITHIN = "within"
    ORDERING = "ordering"
    ORDER = "order"
    METADATA = "metadata"
    VALUE_IS_REFERENCE = "value_is_reference"
    TYPE_INSENSITIVE = "type_insensitive"
    ROUND_VALUES = "round_values"
