from cdisc_rules_engine.enums.base_enum import BaseEnum


class RuleTypes(BaseEnum):
    RELATIONSHIP_INTEGRITY = "Relationship Integrity Check"
    VARIABLE_PRESENCE = "Variable Presence"
    VALUE_PRESENCE = "Value Presence"
    RANGE_CHECK = "Range & Limit"
    DATASET_METADATA_CHECK = "Dataset Metadata Check"
    DATASET_METADATA_CHECK_AGAINST_DEFINE = "Dataset Metadata Check against Define XML"
    VARIABLE_METADATA_CHECK_AGAINST_DEFINE = (
        "Variable Metadata Check against Define XML"
    )
    VALUE_LEVEL_METADATA_CHECK_AGAINST_DEFINE = (
        "Value Level Metadata Check against Define XML"
    )
    DATASET_CONTENTS_CHECK_AGAINST_DEFINE_AND_LIBRARY = (
        "Dataset Contents Check against Define XML and Library Metadata"
    )
    VARIABLE_METADATA_CHECK = "Variable Metadata Check"
    DOMAIN_PRESENCE_CHECK = "Domain Presence Check"
    DATA_PATTERN_AND_FORMAT = "Data Pattern and Format"
    EXTERNAL_DICTIONARIES = "External Dictionaries"
    FUNCTIONAL_DEPENDENCY = "Functional Dependency"
    DATE_ARITHMETIC = "Date Arithmetic"
    DATA_DOMAIN_AGGREGATION = "Data Domain Aggregation"
    DEFINE = "Define-XML"
    POPULATED_VALUES = "Populated Values"
    UNIQUENESS = "Uniqueness"
    VARIABLE_LENGTH = "Variable Length"
    VARIABLE_ORDER = "Variable Order"
    CONSISTENCY = "Consistency"
