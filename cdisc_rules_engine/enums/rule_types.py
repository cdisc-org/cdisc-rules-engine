from cdisc_rules_engine.enums.base_enum import BaseEnum


class RuleTypes(BaseEnum):
    DATASET_CONTENTS_CHECK_AGAINST_DEFINE_AND_LIBRARY = (
        "Dataset Contents Check against Define XML and Library Metadata"
    )
    DATASET_METADATA_CHECK = "Dataset Metadata Check"
    DATASET_METADATA_CHECK_AGAINST_DEFINE = "Dataset Metadata Check against Define XML"
    DEFINE_ITEM_GROUP_METADATA_CHECK = "Define Item Group Metadata Check"
    DEFINE_ITEM_METADATA_CHECK = "Define Item Metadata Check"
    DOMAIN_PRESENCE_CHECK = "Domain Presence Check"
    VALUE_LEVEL_METADATA_CHECK_AGAINST_DEFINE = (
        "Value Level Metadata Check against Define XML"
    )
    VARIABLE_METADATA_CHECK = "Variable Metadata Check"
    VARIABLE_METADATA_CHECK_AGAINST_DEFINE = (
        "Variable Metadata Check against Define XML"
    )
    VARIABLE_METADATA_CHECK_AGAINST_DEFINE_XML_AND_LIBRARY = (
        "Variable Metadata Check against Define XML and Library Metadata"
    )
    VARIABLE_METADATA_CHECK_AGAINST_LIBRARY = (
        "Variable Metadata Check against Library Metadata"
    )
    VALUE_CHECK_AGAINST_DEFINE_XML_VARIABLE = "Value Check against Define XML Variable"
    VALUE_CHECK_AGAINST_DEFINE_XML_VLM = "Value Check against Define XML VLM"
    DEFINE_ITEM_METADATA_CHECK_AGAINST_LIBRARY = (
        "Define Item Metadata Check against Library Metadata"
    )
