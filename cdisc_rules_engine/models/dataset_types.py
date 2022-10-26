from cdisc_rules_engine.enums.base_enum import BaseEnum


class DatasetTypes(BaseEnum):
    CONTENTS = "contents"
    METADATA = "metadata"
    RAW_METADATA = "raw_metadata"
    VARIABLES_METADATA = "variables_metadata"
