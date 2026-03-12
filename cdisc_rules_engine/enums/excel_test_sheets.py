from cdisc_rules_engine.enums.base_enum import BaseEnum


class ExcelDataSheets(BaseEnum):
    DATASETS_SHEET_NAME = "Datasets"
    DATASET_FILENAME_COLUMN = "Filename"
    DATASET_LABEL_COLUMN = "Label"
    DATASETS_SHEET_REQUIRED_COLUMNS = ("Filename", "Label")
