from engine.enums.base_enum import BaseEnum


class DatasetTypes(BaseEnum):
    CONTENTS = "contents"
    METADATA = "metadata"
    JSON_METADATA = "json_metadata"
    VARIABLES_METADATA = "variables_metadata"
